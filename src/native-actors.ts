/** Import-light host registration only; no guest action or execution authority. */
export { registerNativeActorHandler } from "./actors/native-handler.js";
export type {
  NativeActorBinding,
  NativeActorContext,
  NativeActorHandler,
  NativeActorRequest,
} from "./actors/native-handler.js";
