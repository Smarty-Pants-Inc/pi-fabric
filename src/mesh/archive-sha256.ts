// SHA-256's chaining words plus its partial block are a resumable, portable checkpoint.
// Native node:crypto Hash cannot be serialized; final digests remain standard SHA-256.
export interface Sha256State { words: number[]; bytes: number; tail: string }
const INITIAL = [0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19];
const K = [
  0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
  0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
  0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
  0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
  0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
  0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
  0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
  0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2,
];
const rotate = (n: number, bits: number): number => (n >>> bits) | (n << (32 - bits));
export class ArchiveSha256 {
  #words: number[];
  #bytes: number;
  #tail: Buffer;
  constructor(state?: Sha256State) {
    if (state && (!Array.isArray(state.words) || state.words.length !== 8 ||
      !state.words.every(n => Number.isInteger(n) && n >= 0 && n <= 0xffffffff) ||
      !Number.isSafeInteger(state.bytes) || state.bytes < 0 || typeof state.tail !== "string" ||
      !/^(?:[0-9a-f]{2}){0,63}$/.test(state.tail) || state.tail.length / 2 !== state.bytes % 64)) {
      throw new Error("Invalid archive SHA-256 checkpoint");
    }
    this.#words = state ? [...state.words] : [...INITIAL];
    this.#bytes = state?.bytes ?? 0;
    this.#tail = Buffer.from(state?.tail ?? "", "hex");
  }
  update(bytes: Buffer): this {
    this.#bytes += bytes.length;
    const data = this.#tail.length ? Buffer.concat([this.#tail, bytes]) : bytes;
    let offset = 0;
    for (; offset + 64 <= data.length; offset += 64) this.#block(data, offset);
    this.#tail = Buffer.from(data.subarray(offset));
    return this;
  }
  state(): Sha256State { return { words: [...this.#words], bytes: this.#bytes, tail: this.#tail.toString("hex") }; }
  digest(): string {
    const copy = new ArchiveSha256(this.state());
    const pad = Buffer.alloc(this.#tail.length < 56 ? 64 - this.#tail.length : 128 - this.#tail.length);
    pad[0] = 0x80;
    pad.writeUInt32BE(Math.floor(this.#bytes / 0x20000000), pad.length - 8);
    pad.writeUInt32BE((this.#bytes * 8) >>> 0, pad.length - 4);
    copy.update(pad);
    return copy.#words.map(n => n.toString(16).padStart(8, "0")).join("");
  }
  #block(bytes: Buffer, offset: number): void {
    const w = new Int32Array(64);
    for (let i = 0; i < 16; i++) w[i] = bytes.readInt32BE(offset + i * 4);
    for (let i = 16; i < 64; i++) {
      const x = w[i - 15]!, y = w[i - 2]!;
      w[i] = (rotate(x, 7) ^ rotate(x, 18) ^ (x >>> 3)) + w[i - 16]! +
        (rotate(y, 17) ^ rotate(y, 19) ^ (y >>> 10)) + w[i - 7]!;
    }
    let [a,b,c,d,e,f,g,h] = this.#words as [number,number,number,number,number,number,number,number];
    for (let i = 0; i < 64; i++) {
      const t1 = (h + (rotate(e,6) ^ rotate(e,11) ^ rotate(e,25)) + ((e & f) ^ (~e & g)) + K[i]! + w[i]!) | 0;
      const t2 = ((rotate(a,2) ^ rotate(a,13) ^ rotate(a,22)) + ((a & b) ^ (a & c) ^ (b & c))) | 0;
      h=g; g=f; f=e; e=(d+t1)|0; d=c; c=b; b=a; a=(t1+t2)|0;
    }
    const next = [a,b,c,d,e,f,g,h];
    this.#words = this.#words.map((n,i) => (n + next[i]!) >>> 0);
  }
}
