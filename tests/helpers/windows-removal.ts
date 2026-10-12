/**
 * pi-fabric#952 + smarty-dev#7858: on win32 a live actor removal never deletes the actor's tree. The tree delete runs
 * only under the root's Main publication fence, and the fence needs flock, which Windows lacks; there the removal
 * revokes the actor and its cleanup stays pending with the #7858 refusal below (the offline removal refuses the same
 * way). Tests that need the tree delete to finish are gated with this predicate, as pi-fabric#913 gated its own.
 */
export const removalTreeDeleteRefused = process.platform === "win32";
export const REMOVAL_REFUSED_7858 = /actor removal is not supported where file ownership cannot be proven \(smarty-dev#7858\)/;
