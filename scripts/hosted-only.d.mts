export const fleetHostMarker: "PI_TEST_FLEET_HOST";
export const hostedAdmissionMarker: "PI_TEST_HOSTED_ONLY_PARENT_ADMITTED";
export function rememberFleetHost(env?: NodeJS.ProcessEnv): void;
/** markerPath/files are test seams; production callers omit them to use the fixed recipe path and real filesystem. */
export function isHostedOnlyAllowed(
  env: NodeJS.ProcessEnv,
  directory?: (path: string) => string | undefined,
  markerPath?: string,
  files?: {
    lstatSync(path: string): { isFile(): boolean; uid: number; mode: number };
    readFileSync(path: string, encoding: "utf8"): string;
  },
): boolean;
export function hostedOnly(caseName: string, env?: NodeJS.ProcessEnv): boolean;
