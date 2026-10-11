/** Crash-path result publication for the worker custodian.
 *
 * A crash result is not an exit receipt: it is published (and the custodian
 * exits) only after every admitted execution obligation has drained. If that
 * drain itself fails, the custodian keeps execution custody, but it still
 * publishes a terminal failure that names both the crash and the cleanup
 * failure, so its parent never waits on a silent worker. */
export interface CrashFinisherOptions {
  cleanup: () => Promise<void>;
  settled: () => Promise<void>;
  report: (error: unknown) => void;
  exit: (code: number) => void;
  log: (text: string) => void;
  retain: () => void;
}

const describe = (error: unknown): string => error instanceof Error ? error.message : String(error);

export const createCrashFinisher = (options: CrashFinisherOptions) => {
  let pending = false;
  const finish = async (error: unknown): Promise<void> => {
    if (pending) return;
    pending = true;
    options.log(error instanceof Error ? error.stack ?? error.message : String(error));
    try {
      await options.cleanup();
      await options.settled();
    } catch (cleanupError) {
      options.log(`Crash cleanup unresolved; retaining execution custody: ${String(cleanupError)}`);
      try { options.report(new Error(`${describe(error)}; crash cleanup unresolved: ${describe(cleanupError)}`)); }
      finally { options.retain(); }
      return;
    }
    options.report(error);
    options.exit(1);
  };
  return { finish, get pending() { return pending; } };
};
