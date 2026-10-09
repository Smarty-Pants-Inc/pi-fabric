// natsbench: JetStream durability/latency bench for smarty-dev#7504.
//
//	natsbench -mode load -url nats://127.0.0.1:PORT -case kv|pub|batch -c N -warm 5s -dur 30s
//	natsbench -mode kill -server ./nats-server -conf FILE -url ... -rounds 3 -c 8
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	mrand "math/rand/v2"
	"net"
	"os"
	"os/exec"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	mode    = flag.String("mode", "load", "load|kill")
	url     = flag.String("url", "nats://127.0.0.1:4222", "")
	cse     = flag.String("case", "kv", "kv|pub|batch")
	conc    = flag.Int("c", 1, "concurrent writers")
	warm    = flag.Duration("warm", 5*time.Second, "")
	dur     = flag.Duration("dur", 30*time.Second, "")
	label   = flag.String("label", "", "backend label")
	server  = flag.String("server", "./nats-server", "")
	conf    = flag.String("conf", "", "")
	rounds  = flag.Int("rounds", 3, "")
	keys    = 10000
	payload = make([]byte, 200)
	msg1k   = make([]byte, 1024)
)

func mustJS() (*nats.Conn, jetstream.JetStream) {
	nc, err := nats.Connect(*url, nats.MaxReconnects(0))
	if err != nil {
		panic(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}
	return nc, js
}

func pct(s []time.Duration, p float64) float64 {
	if len(s) == 0 {
		return 0
	}
	i := int(p * float64(len(s)))
	if i >= len(s) {
		i = len(s) - 1
	}
	return float64(s[i].Microseconds()) / 1000.0
}

func load() {
	ctx := context.Background()
	nc, js := mustJS()
	defer nc.Close()
	bucket := "B" + *cse
	_ = js.DeleteKeyValue(ctx, bucket)
	_ = js.DeleteStream(ctx, "S")
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: bucket, History: 1, Storage: jetstream.FileStorage}); err != nil {
		panic(err)
	}
	if *cse == "abatch" { // NATS 2.12 atomic batch publish (ADR-50) on the KV stream
		si, err := js.Stream(ctx, "KV_"+bucket)
		if err != nil {
			panic(err)
		}
		cfg := si.CachedInfo().Config
		cfg.AllowAtomicPublish = true
		if _, err := js.UpdateStream(ctx, cfg); err != nil {
			panic(err)
		}
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "S", Subjects: []string{"ev.>"}, Storage: jetstream.FileStorage}); err != nil {
		panic(err)
	}
	var measuring atomic.Bool
	var stop atomic.Bool
	lat := make([][]time.Duration, *conc)
	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			nc, js := mustJS()
			defer nc.Close()
			kv, err := js.KeyValue(ctx, bucket)
			if err != nil {
				panic(err)
			}
			buf := make([]time.Duration, 0, 1<<20)
			r := mrand.New(mrand.NewPCG(uint64(w), 7))
			for !stop.Load() {
				t0 := time.Now()
				switch *cse {
				case "kv":
					_, err = kv.Put(ctx, fmt.Sprintf("k%d", r.IntN(keys)), payload)
				case "pub":
					_, err = js.Publish(ctx, fmt.Sprintf("ev.%d", w), msg1k)
				case "abatch":
					// One atomic batch: 2 plain publishes + a commit publish that waits for the ack.
					id := fmt.Sprintf("w%d-%d", w, r.Uint64())
					for i := 1; i <= 3 && err == nil; i++ {
						m := nats.NewMsg(fmt.Sprintf("$KV.%s.k%d", bucket, r.IntN(keys)))
						m.Data = payload
						m.Header.Set("Nats-Batch-Id", id)
						m.Header.Set("Nats-Batch-Sequence", fmt.Sprint(i))
						if i < 3 {
							err = nc.PublishMsg(m)
						} else {
							m.Header.Set("Nats-Batch-Commit", "1")
							var resp *nats.Msg
							if resp, err = nc.RequestMsg(m, 10*time.Second); err == nil {
								var ack struct {
									Error *struct{ Description string } `json:"error"`
									Batch string                         `json:"batch"`
									Count int                            `json:"count"`
								}
								if err = json.Unmarshal(resp.Data, &ack); err == nil && (ack.Error != nil || ack.Batch != id || ack.Count != 3) {
									err = fmt.Errorf("bad batch ack: %s", resp.Data)
								}
							}
						}
					}
				case "batch":
					var fs [3]jetstream.PubAckFuture
					for i := range fs {
						fs[i], err = js.PublishAsync(fmt.Sprintf("$KV.%s.k%d", bucket, r.IntN(keys)), payload)
						if err != nil {
							break
						}
					}
					for i := 0; err == nil && i < 3; i++ {
						select {
						case <-fs[i].Ok():
						case e := <-fs[i].Err():
							err = e
						}
					}
				}
				if err != nil {
					panic(err)
				}
				if measuring.Load() {
					buf = append(buf, time.Since(t0))
				}
			}
			lat[w] = buf
		}(w)
	}
	time.Sleep(*warm)
	measuring.Store(true)
	t0 := time.Now()
	time.Sleep(*dur)
	measuring.Store(false)
	el := time.Since(t0)
	stop.Store(true)
	wg.Wait()
	var all []time.Duration
	for _, b := range lat {
		all = append(all, b...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	out := map[string]any{"backend": *label, "case": *cse, "c": *conc, "ops": len(all),
		"secs": el.Seconds(), "ops_s": float64(len(all)) / el.Seconds(),
		"p50_ms": pct(all, .5), "p99_ms": pct(all, .99), "p999_ms": pct(all, .999)}
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
	_ = js.DeleteKeyValue(ctx, bucket)
	_ = js.DeleteStream(ctx, "S")
}

// ---- kill -9 mode ----

func startServer() (*exec.Cmd, time.Time) {
	cmd := exec.Command(*server, "-c", *conf)
	cmd.Stdout, cmd.Stderr = nil, nil
	t := time.Now()
	if err := cmd.Start(); err != nil {
		panic(err)
	}
	fmt.Fprintf(os.Stderr, "nats-server pid %d\n", cmd.Process.Pid)
	return cmd, t
}

// waitReady returns once the KV bucket answers a Status call.
func waitReady(t0 time.Time) (tcp, ready time.Duration) {
	host := (*url)[len("nats://"):]
	for {
		c, err := net.DialTimeout("tcp", host, 100*time.Millisecond)
		if err == nil {
			c.Close()
			tcp = time.Since(t0)
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	for {
		nc, err := nats.Connect(*url, nats.MaxReconnects(0))
		if err == nil {
			js, _ := jetstream.New(nc)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			kv, err := js.KeyValue(ctx, "KILL")
			if err == nil {
				_, err = kv.Status(ctx)
			}
			cancel()
			nc.Close()
			if err == nil {
				return tcp, time.Since(t0)
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func kill() {
	ctx := context.Background()
	cmd, t0 := startServer()
	waitTCP(t0)
	nc, js := mustJS()
	_ = js.DeleteKeyValue(ctx, "KILL")
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "KILL", History: 1, Storage: jetstream.FileStorage}); err != nil {
		panic(err)
	}
	nc.Close()
	type acked struct{ k, v string }
	var total []acked
	for round := 1; round <= *rounds; round++ {
		var mu sync.Mutex
		var got []acked
		var wg sync.WaitGroup
		for w := 0; w < *conc; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				nc, js := mustJS()
				defer nc.Close()
				kv, err := js.KeyValue(ctx, "KILL")
				if err != nil {
					panic(err)
				}
				for n := 0; ; n++ {
					k := fmt.Sprintf("r%d.w%d.n%d", round, w, n)
					v := make([]byte, 100)
					rand.Read(v)
					sv := fmt.Sprintf("%x", v) // 200 bytes
					c2, cancel := context.WithTimeout(ctx, 2*time.Second)
					_, err := kv.Put(c2, k, []byte(sv))
					cancel()
					if err != nil {
						return // server gone
					}
					mu.Lock()
					got = append(got, acked{k, sv})
					mu.Unlock()
				}
			}(w)
		}
		time.Sleep(3*time.Second + time.Duration(mrand.IntN(4000))*time.Millisecond)
		mu.Lock()
		before := len(got)
		mu.Unlock()
		pid := cmd.Process.Pid
		_ = syscall.Kill(pid, syscall.SIGKILL)
		_ = cmd.Wait()
		wg.Wait()
		total = append(total, got...)
		cmd, t0 = startServer()
		tcp, ready := waitReady(t0)
		// verify every acked write ever (all rounds so far)
		nc, js := mustJS()
		kv, _ := js.KeyValue(ctx, "KILL")
		missing, wrong := 0, 0
		for _, a := range total {
			e, err := kv.Get(ctx, a.k)
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				missing++
				continue
			} else if err != nil {
				panic(err)
			}
			if string(e.Value()) != a.v {
				wrong++
			}
		}
		st, _ := kv.Status(ctx)
		nc.Close()
		b, _ := json.Marshal(map[string]any{"round": round, "killed_pid": pid, "acked_this_round": len(got),
			"acked_at_kill_signal": before, "acked_total": len(total), "missing": missing, "wrong_value": wrong,
			"bucket_values": st.Values(), "restart_tcp_ms": tcp.Milliseconds(), "restart_ready_ms": ready.Milliseconds()})
		fmt.Println(string(b))
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	_ = cmd.Wait()
}

func waitTCP(t0 time.Time) {
	host := (*url)[len("nats://"):]
	for {
		if c, err := net.DialTimeout("tcp", host, 100*time.Millisecond); err == nil {
			c.Close()
			time.Sleep(200 * time.Millisecond)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func main() {
	flag.Parse()
	rand.Read(payload)
	rand.Read(msg1k)
	if *mode == "kill" {
		kill()
	} else {
		load()
	}
}
