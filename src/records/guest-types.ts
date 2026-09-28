/**
 * The records.* guest declarations (smarty-dev#754). Kept out of GUEST_TYPE_DECLARATIONS, which
 * the startup graph parses, and appended by the TypeScript kernel (loaded lazily) and by
 * guestDeclarationsWithRecords for tests and docs checks.
 */
export const RECORDS_GUEST_DECLARATIONS = `
type FabricRecordKind = "issue" | "status" | "comment" | "decision" | "ask" | "answer" | "handoff" | "link" | "close" | "reopen" | "mirror";
interface FabricRecordData {
  title?: string; body?: string; owner?: string; acceptance?: string; labels?: string[]; nextAction?: string; stage?: string;
  eta?: string | { stage: string; at: string }[]; state?: "in progress" | "waiting" | "blocked" | "done" | "pending" | "mirrored" | "refused" | "skipped" | "unknown"; waitOn?: string;
  deleted?: boolean; by?: string; where?: string; to?: string; class?: string; minutes?: number;
  ask?: string; outcome?: "answered" | "withdrawn"; key?: string;
  pr?: string; issue?: string; commit?: string; url?: string; forge?: string; number?: number;
  reason?: string; mirrorOf?: string; target?: string; githubId?: string; attempts?: number; error?: string;
  via?: string;
}
interface FabricRecordsAppendArgs {
  ref?: string;
  repo?: string;
  kind: FabricRecordKind;
  key: string;
  text?: string;
  data?: FabricRecordData;
  supersedes?: string;
  author?: string;
}
interface FabricRecordReceipt { id: string; sequence: number; origin: string; topic: string; ref: string; key: string; createdAt: number }
interface FabricRecord {
  id: string; org: string; origin: string; sequence: number; ref: string; topic: string; kind: FabricRecordKind;
  from: string; fromName?: string; createdAt: number; text?: string; data: FabricRecordData; supersedes?: string; key: string;
}
interface FabricRecordsPage { records: FabricRecord[]; next: number; frontier: number; origin: string }
interface FabricRecordFold {
  title?: string; body?: string; owner?: string; acceptance?: string; labels?: string[]; nextAction?: string; stage?: string; open: boolean;
  statuses: Record<string, { id: string; at: number; text?: string; state?: string; eta?: unknown; waitOn?: string; name?: string }>;
  decisions: FabricRecord[]; openAsks: FabricRecord[]; links: FabricRecord[]; mirror: Record<string, FabricRecordData>;
  truncated?: string[];
  more?: Partial<Record<FabricRecordFoldPart, string>>;
}
type FabricRecordFoldPart = "statuses" | "mirror" | "decisions" | "openAsks" | "links";
interface FabricRecordsListItem {
  ref: string; title?: string; owner?: string; stage?: string; open: boolean; updatedAt: number;
  statuses: Record<string, { at: number; state?: string; eta?: unknown }>; statusCount: number;
  openAsks: { id: string; to?: string; at: number }[]; openAskCount: number;
}
/** The org record; see the fabric-exec records reference. Retry append with the same key. */
interface FabricRecordsApi {
  append(args: FabricRecordsAppendArgs): Promise<FabricRecordReceipt>;
  read(args?: { after?: number; limit?: number; origin?: string; ref?: string; kind?: FabricRecordKind; to?: string }): Promise<FabricRecordsPage>;
  get(args: { ref: string; after?: number; limit?: number }): Promise<{ ref: string; state?: FabricRecordFold; history: FabricRecord[]; next?: number }>;
  fold(args: { ref: string; part: FabricRecordFoldPart; after?: string }): Promise<{ ref: string; part: FabricRecordFoldPart; items: unknown[]; next?: string }>;
  list(args?: { org?: string; repo?: string; open?: boolean; owner?: string; hasOpenAsk?: boolean; updatedSince?: number; limit?: number; after?: string }): Promise<{ items: FabricRecordsListItem[]; next?: string }>;
  status(): Promise<{ org: string; origin: string; frontier: number; unpublished: number; admission: { state: "ok" | "alarm" | "refuse" | "disabled"; lagSeconds?: number; frontier?: string; insertLsn?: string }; statusFile: string }>;
}

declare const records: FabricRecordsApi;
`;

/** The full guest declarations a kernel checks against: the base ones plus records. */
export const withRecordsDeclarations = (declarations: string): string => `${declarations}\n${RECORDS_GUEST_DECLARATIONS}`;
