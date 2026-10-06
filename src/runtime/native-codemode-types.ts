export const NATIVE_CODEMODE_TYPES = `
type NativeModelType = "chat" | "image" | "classifier";
interface NativeModelInfo { type?: NativeModelType; provider: string; id: string; name: string; api: string; input: ("text" | "image")[]; contextWindow?: number; [key: string]: unknown }
type NativeModelSelector = { provider: string; id: string };
type NativeClassifierQuestion =
 | { type: "choice"; instructions: string; criteria: Record<string, string> }
 | { type: "score"; instructions: string; criteria: string[] }
 | { type: "bool"; instructions: string; criteria: { true: string; false: string } };
type NativeClassifierAnswer =
 | { type: "choice"; choice: string; probabilities: Record<string, number>; confidence: number }
 | { type: "score"; score: number; confidence: number }
 | { type: "bool"; probability: number };
interface NativeClassifierContext { state: Record<string, unknown>; questions: Record<string, NativeClassifierQuestion> }
interface NativeModelUsage { input: number; output: number; totalTokens: number; cost: { total: number } }
interface NativeClassifierResult { provider: string; model: string; answers: Record<string, NativeClassifierAnswer>; usage?: NativeModelUsage; stopReason: "stop" | "error" | "aborted"; errorMessage?: string }
type NativeImageBlock = { type: "image"; data: string; mimeType: string };
type NativeTextBlock = { type: "text"; text: string };
interface NativeImagesContext { input: (NativeImageBlock | NativeTextBlock)[] }
interface NativeImagesResult { provider: string; model: string; output: (NativeImageBlock | NativeTextBlock)[]; usage?: NativeModelUsage; stopReason: "stop" | "error" | "aborted"; errorMessage?: string }
interface FabricNativeModels {
 (): Promise<FabricModelInfo[]>;
 getModelsOfType(type: NativeModelType, provider?: string): Promise<NativeModelInfo[]>;
 getAvailableOfType(type: NativeModelType, provider?: string): Promise<NativeModelInfo[]>;
 getModelOfType(type: NativeModelType, provider: string, id: string): Promise<NativeModelInfo | undefined>;
 classify(model: NativeModelSelector, context: NativeClassifierContext): Promise<NativeClassifierResult>;
 generateImages(model: NativeModelSelector, context: NativeImagesContext): Promise<NativeImagesResult>;
}
interface NativeToolInfo { name: string; description: string }
declare const ALL_TOOLS: readonly NativeToolInfo[];
declare function searchTools(query: string, options?: { limit?: number; namespace?: string }): Promise<NativeToolInfo[]>;
declare function describeTool(name: string): Promise<(NativeToolInfo & { declaration: string }) | undefined>;
declare function describeNamespace(name: string): Promise<{ name: string; description?: string; instructions?: string; tools: NativeToolInfo[] } | undefined>;
declare const models: FabricNativeModels;
declare function store(key: string, value: unknown): void;
declare function load<T = unknown>(key: string): T | undefined;
declare function text(value: unknown): void;
declare function image(value: NativeImageBlock | string | { image_url: string | { url: string } }): void;
declare function exit(): never;
`;
