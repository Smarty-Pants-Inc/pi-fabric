import { expect, it } from "vitest";
import { scanCommand } from "../src/core/pattern-kill.js";

// Scanner DATA only: diagnostic argv and quoted words are not shell control tokens.
const pairs = [
  ["diagnostic then/fi argv retains skipped PID overwrite",
    `P=$(pgrep worker); if false; then echo then fi; P=4242; fi; kill "$P"`,
    `P=$(pgrep worker); if false; then echo then fi; P=4242; fi; P=4242; kill "$P"`, "kill"],
  ["diagnostic then/fi argv retains skipped tmp overwrite",
    `D=/tmp; if false; then echo then fi; D=/own; fi; rm -rf "$D"`,
    `D=/tmp; if false; then echo then fi; D=/own; fi; D=/own; rm -rf "$D"`, "tmp"],
  ["quoted and escaped fi do not close skipped PID scope",
    String.raw`P=$(pgrep worker); if false; then "fi"; f\i; P=4242; fi; kill "$P"`,
    String.raw`P=$(pgrep worker); if false; then "fi"; f\i; P=4242; fi; P=4242; kill "$P"`, "kill"],
  ["quoted and escaped fi do not close skipped tmp scope",
    String.raw`D=/tmp; if false; then "fi"; f\i; D=/own; fi; rm -rf "$D"`,
    String.raw`D=/tmp; if false; then "fi"; f\i; D=/own; fi; D=/own; rm -rf "$D"`, "tmp"],
  ["quoted if executable does not bind its PID-looking argv",
    `P=$(pgrep worker); "if" P=4242; kill "$P"`,
    `P=$(pgrep worker); "if" P=4242; P=4242; kill "$P"`, "kill"],
  ["quoted if executable does not bind its tmp-looking argv",
    `D=/tmp; "if" D=/own; rm -rf "$D"`,
    `D=/tmp; "if" D=/own; D=/own; rm -rf "$D"`, "tmp"],
  ["case nonmatch retains tmp source until guaranteed overwrite",
    `D=/tmp; case x in y) D=/own;; esac; rm -rf "$D"`,
    `D=/tmp; case x in y) D=/own;; esac; D=/own; rm -rf "$D"`, "tmp"],
  ["zero-until retains tmp source until guaranteed overwrite",
    `D=/tmp; until true; do D=/own; done; rm -rf "$D"`,
    `D=/tmp; until true; do D=/own; done; D=/own; rm -rf "$D"`, "tmp"],
] as const;

for (const [name, refused, allowed, policy] of pairs) {
  it(`${name} — refuse`, () => {
    const verdict = scanCommand(refused);
    expect(verdict.exhausted).toBe(false);
    expect(policy === "kill" ? verdict.blocked : verdict.wipe).toBe(true);
  });
  it(`${name} — allow`, () => {
    expect(scanCommand(allowed)).toEqual({ blocked: false, wipe: false, exhausted: false });
  });
}
