// Shared by QuickJS and the trusted Node/Bun executor. No host dependencies.
export const NATIVE_CODEMODE_GUEST = String.raw`
const __nativeModels = Object.assign(() => __call("fabric.$models", {}), {
  getModelsOfType: (type, provider) => __call("native.getModelsOfType", { type, provider }),
  getAvailableOfType: (type, provider) => __call("native.getAvailableOfType", { type, provider }),
  getModelOfType: (type, provider, id) => __call("native.getModelOfType", { type, provider, id }),
  classify: (model, context) => __call("native.classify", { model: { provider: model.provider, id: model.id }, context }),
  generateImages: (model, context) => __call("native.generateImages", { model: { provider: model.provider, id: model.id }, context }),
});
__toolsBase.models = Object.freeze(__nativeModels);
globalThis.models = __toolsBase.models;
globalThis.ALL_TOOLS = Object.freeze([]);
globalThis.searchTools = (query, options = {}) => __call("fabric.$nativeSearch", { query, ...options });
globalThis.describeTool = name => __call("fabric.$nativeDescribe", { name });
globalThis.describeNamespace = name => __call("fabric.$describeNamespace", { name });
let __nativeAllTools = Object.freeze([]);
globalThis.nativeDiscovery = Object.freeze({
  get ALL_TOOLS() { return __nativeAllTools; },
  searchTools: (query, options = {}) => __call("fabric.$piSearch", { query, ...options }),
  describeTool: name => __call("fabric.$piDescribe", { name }),
  describeNamespace: name => __call("fabric.$piNamespace", { name }),
});
if (typeof __nativeProfile !== "undefined" && __nativeProfile) {
  globalThis.searchTools = globalThis.nativeDiscovery.searchTools;
  globalThis.describeTool = globalThis.nativeDiscovery.describeTool;
  globalThis.describeNamespace = globalThis.nativeDiscovery.describeNamespace;
}
const __exit = Object.freeze({});
let __storeValues;
let __storeDirty = false;
let __storeError;
const __cloneJson = value => JSON.parse(JSON.stringify(value));
globalThis.load = key => {
  if (typeof key !== "string") throw new Error("load key must be a string");
  if (!__storeValues) throw (__storeError ?? new Error("Script store is unavailable in this runtime"));
  return Object.prototype.hasOwnProperty.call(__storeValues, key) ? __cloneJson(__storeValues[key]) : undefined;
};
globalThis.store = (key, value) => {
  if (typeof key !== "string") throw new Error("store key must be a string");
  if (!__storeValues) throw (__storeError ?? new Error("Script store is unavailable in this runtime"));
  const next = Object.assign(Object.create(null), __storeValues);
  if (value === undefined) delete next[key];
  else {
    const json = JSON.stringify(value);
    if (json === undefined) throw new Error("Script store values must be JSON");
    if (json.length > 262144) throw new Error("Script store value exceeds 262144 characters");
    next[key] = JSON.parse(json);
  }
  if (Object.values(next).reduce((sum, item) => sum + JSON.stringify(item).length, 0) > 1048576) throw new Error("Script store exceeds 1048576 characters");
  __storeValues = next;
  __storeDirty = true;
};
const __emitted = [];
Object.defineProperty(globalThis, "__fabricEmitted", { value: __emitted });
let __emittedChars = 0;
let __emittedCalls = 0;
const __countOutput = chars => {
  __emittedChars += chars;
  if (++__emittedCalls > 100000 || __emittedChars > 16777216) throw new Error("Script output limit exceeded");
};
globalThis.text = value => {
  const text = typeof value === "string" ? value : JSON.stringify(value);
  __countOutput((text ?? "").length);
  print(text);
};
globalThis.image = value => {
  if (value && typeof value === "object" && "image_url" in value) value = typeof value.image_url === "string" ? value.image_url : value.image_url?.url;
  if (typeof value === "string") {
    const match = /^data:(image\/(?:png|jpeg|gif|webp));base64,([A-Za-z0-9+/=\s]+)$/.exec(value);
    if (!match) throw new Error("image() requires a local base64 PNG, JPEG, GIF or WebP; remote URLs are unsupported");
    value = { type: "image", mimeType: match[1], data: match[2].replace(/\s/g, "") };
  }
  if (!value || value.type !== "image" || !/^image\/(png|jpeg|gif|webp)$/.test(value.mimeType) || typeof value.data !== "string" || !/^[A-Za-z0-9+/=\s]+$/.test(value.data)) throw new Error("Invalid image block");
  __countOutput(value.data.length);
  const block = { type: "image", mimeType: value.mimeType, data: value.data };
  __emitted.push(block);
  if (typeof globalThis.__fabricEmitImage === "function") globalThis.__fabricEmitImage(block);
};
globalThis.exit = () => { throw __exit; };
globalThis.__fabricRun = async (main) => {
  if (typeof __nativeToolsEnabled !== "undefined" && __nativeToolsEnabled) {
    __nativeAllTools = Object.freeze(await __call("fabric.$piAllTools", {}));
    for (const tool of __nativeAllTools) __nativeToolNames.add(tool.name);
    __nativeCatalogReady = true;
    if (__nativeProfile) globalThis.ALL_TOOLS = __nativeAllTools;
  }
  if (typeof __nativeStoreEnabled !== "undefined" && __nativeStoreEnabled) {
    if (!(typeof __nativeProfile !== "undefined" && __nativeProfile)) globalThis.ALL_TOOLS = Object.freeze(await __call("fabric.$allTools", {}));
    try { __storeValues = Object.assign(Object.create(null), await __call("native.load", {})); }
    catch (error) { __storeError = error; }
  }
  let value;
  try { value = await main(); } catch (error) { if (error !== __exit) throw error; }
  if (__storeDirty) await __call("native.store", { values: __storeValues });
  return value;
};
`;
