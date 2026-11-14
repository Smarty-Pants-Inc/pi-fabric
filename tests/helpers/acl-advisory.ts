// Keep: Windows job "Windows affected (8/10)" 132041182691 caught this class: https://github.com/Smarty-Pants-Inc/pi-fabric/pull/1960
export function stripAclAdvisory(output: string): string {
  return output.replace(/^\[pi-fabric\] FABRIC_MESH_STATE_ACL_ADVISORY: [^\r\n]*(?:\r?\n|$)/gm, "");
}
