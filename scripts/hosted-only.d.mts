export const fleetHostMarker: "PI_TEST_FLEET_HOST";
export const hostedAdmissionMarker: "PI_TEST_HOSTED_ONLY_PARENT_ADMITTED";
export function rememberFleetHost(env?: NodeJS.ProcessEnv): void;
export function isHostedOnlyAllowed(env: NodeJS.ProcessEnv, directory?: (path: string) => string | undefined): boolean;
export function hostedOnly(caseName: string, env?: NodeJS.ProcessEnv): boolean;
