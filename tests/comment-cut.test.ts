import { describe, expect, it } from "vitest";
import { cutsCommentList } from "../src/core/comment-cut.js";

// smarty-dev#1469: the real commands from the census, plus the reads that must stay allowed.
const GHR = "env -u GITHUB_TOKEN -u GH_TOKEN smarty-github-app-token run Smarty-Pants-Inc --";
const LIST = `${GHR} gh api "repos/Smarty-Pants-Inc/smarty-dev/issues/1201/comments?per_page=100" --paginate`;
const LISTING = `--jq '.[] | {id, user: .user.login, created_at, first: (.body | split("\\n")[0])}'`;

const cuts: Array<[string, string]> = [
  ["first-line listing | tail -n 4",
    `${LIST} --jq '.[] | {id, user:.user.login, at:.created_at, first:(.body|split("\\n")[0][0:160])}' | tail -n 4`],
  ["tail -n +1 then a final tail",
    `${LIST} ${LISTING} | tail -n +1 | grep -n "" | awk -F: '$1>0' | tail -n 6`],
  ["string slice, then 2>&1 | tail -4",
    `${LIST} --jq '.[] | {id, user: .user.login, body: (.body | .[0:200])}' 2>&1 | tail -4`],
  ["the codex case: | tail -15",
    `${GHR} gh api repos/Smarty-Pants-Inc/smarty-dev/issues/1201/comments --jq '.[] | "\\(.user.login) \\(.created_at) \\(.body[0:300])"' | tail -15`],
  ["--jq '.[-1].body'", `${LIST} --jq '.[-1].body'`],
  ["--jq '.[-3:]'", `${LIST} --jq '.[-3:]'`],
  ["a PR's comment list | head -20", `${GHR} gh api repos/o/r/pulls/12/comments ${LISTING} | head -20`],
  ["top-level '.[0:5]'", `${LIST} --jq '.[0:5]'`],
  ["top-level '.[:5]'", `${LIST} --jq '.[:5] | .[].body'`],
  ["jq last as a filter", `${LIST} --jq 'last | .body'`],
  ["jq first as a filter", `${LIST} --jq 'first'`],
  ["jq last(...)", `${LIST} --jq 'last(.[]) | .body'`],
  ["jq limit(...)", `${LIST} --jq 'limit(3; .[])'`],
  ["piped into jq '.[-2:]'", `${LIST} | jq '.[-2:]'`],
  ["sed -n on the list", `${LIST} ${LISTING} | sed -n '1,5p'`],
  ["tail -n +2 drops the first", `${LIST} ${LISTING} | tail -n +2`],
  // review/astra F1 on #104: options before the endpoint, and a pipeline wrapped over lines.
  ["--jq before the endpoint", `${GHR} gh api --paginate --jq '.[-1].body' repos/o/r/issues/1201/comments`],
  ["a newline after the pipe", `${LIST} ${LISTING} |\n  tail -n 15`],
  ["a backslash continuation before | tail", `${LIST} ${LISTING} \\\n  | tail -n 15`],
  ["a backslash continuation before --jq", `${LIST} \\\n  --jq '.[-3:]'`],
  ["a curl read | tail", `curl -s https://api.github.com/repos/o/r/issues/7/comments | jq -c '.[]' | tail -n 5`],
  ["a cut read after an allowed one", `${GHR} gh api repos/o/r/issues/1/comments/77 --jq .body; ${LIST} ${LISTING} | tail -3`],
];

const allowed: Array<[string, string]> = [
  ["the first-line listing", `${LIST} ${LISTING}`],
  ["a string slice .body[0:250]", `${LIST} --jq '.[] | {id, user: .user.login, body: .body[0:250]}'`],
  ["a single comment | head -30",
    `${GHR} gh api repos/Smarty-Pants-Inc/smarty-dev/issues/comments/5852457274 --jq .body | head -30`],
  ["tail -n +1", `${LIST} ${LISTING} | tail -n +1`],
  ["tail +1", `${LIST} ${LISTING} | tail +1`],
  ["a tail on a log after ;", `${LIST} ${LISTING}; grep -n error /var/log/app.log | tail -3`],
  ["a tail on a log after &&", `${LIST} ${LISTING} && tail -n 5 build.log`],
  ["a date filter", `${LIST} --jq '.[] | select(.created_at > "2026-09-27T18:00:00Z") | {id, first: (.body | split("\\n")[0])}'`],
  ["an issue body | head", `${GHR} gh api repos/o/r/issues/1469 --jq .body | head -40`],
  ["an issue list | head", `${GHR} gh api "repos/o/r/issues?state=open" --jq '.[].number' | head -5`],
  ["a slice inside the object", `${LIST} --jq '.[] | {id, first: (.body | split("\\n")[0] | .[0:200])}'`],
  ["a quoted first key", `${LIST} --jq '.[] | {id, "first": (.body | split("\\n")[0])}'`],
  ["a single comment, options first", `${GHR} gh api --jq .body repos/o/r/issues/comments/5 | head -30`],
  ["a tail on a log before ;", `grep -n error app.log | tail -3; ${LIST} ${LISTING}`],
  ["a tail on a log before &&", `tail -n 5 build.log && ${LIST} ${LISTING}`],
  ["a tail on the previous line", `grep -n x notes.txt | tail -n 3\n${LIST} ${LISTING}`],
  ["a head in the previous gh api", `${GHR} gh api repos/o/r/pulls/5 --jq .body | head -3 > b.txt || ${LIST} ${LISTING}`],
  ["options first, then a log tail after &&", `${GHR} gh api --paginate ${LISTING} repos/o/r/issues/7/comments && tail -n 5 build.log`],
  ["a log tail before ; and a curl read", `grep -n error app.log | tail -3; curl -s https://api.github.com/repos/o/r/issues/7/comments`],
  ["a log tail on the line before a curl read", `grep -n error app.log | tail -3\ncurl -s https://api.github.com/repos/o/r/issues/7/comments`],
  ["a tail on the next line", `${LIST} ${LISTING}\ngrep -n x notes.txt | tail -n 3`],
  ["a tail in the next gh api", `${LIST} ${LISTING} > c.json || gh api repos/o/r/pulls/5 --jq .body | tail -3`],
];

describe("cutsCommentList (smarty-dev#1469)", () => {
  it.each(cuts)("blocks %s", (_name, command) => expect(cutsCommentList(command)).toBe(true));
  it.each(allowed)("allows %s", (_name, command) => expect(cutsCommentList(command)).toBe(false));
});
