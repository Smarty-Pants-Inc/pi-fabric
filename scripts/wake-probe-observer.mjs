// Test-only event-driven native probe observation. No sampling interval or poll.
import { EventEmitter } from 'node:events';

export function createWakeProbeObserver() {
  const events = new EventEmitter();
  const waits = new Set();
  let failure;
  const fail = error => {
    failure ??= error;
    for (const reject of [...waits]) reject(failure);
  };
  const observe = (check, label, timeout, quiet) => new Promise((resolve, reject) => {
    let timer;
    const finish = error => {
      clearTimeout(timer);
      events.off('change', changed);
      waits.delete(cancel);
      if (error) reject(error); else resolve();
    };
    const cancel = error => finish(error);
    const changed = () => {
      try {
        if (failure) throw failure;
        const ready = check();
        if (!quiet && ready) finish();
      } catch (error) { finish(error); }
    };
    // Subscribe before checking: an arrival between submission and observation
    // is either visible in the initial snapshot or wakes this exact waiter.
    waits.add(cancel);
    events.on('change', changed);
    timer = setTimeout(() => {
      if (quiet) {
        try { check(); finish(failure); } catch (error) { finish(error); }
      } else finish(new Error('Timed out: ' + label));
    }, Math.max(0, timeout));
    changed();
  });
  return {
    notify: () => events.emit('change'),
    fail,
    waitFor: (predicate, label, timeout = 15000) => observe(predicate, label, timeout, false),
    quiet: (check, label, duration) => observe(check, label, duration, true),
    close: () => fail(new Error('Probe observer closed')),
  };
}
