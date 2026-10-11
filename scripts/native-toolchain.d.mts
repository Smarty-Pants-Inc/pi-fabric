export interface ZigPin { url: string; sha256: string }
export const TOOLCHAIN: {
  zig: { version: string } & Record<string, ZigPin>;
  expected: Record<string, Record<string, string>>;
};
export function nativeArch(): string;
export function cacheRoot(): string;
export function untrustedCache(root?: string): string | undefined;
export function pinnedTarball(): { path: string; reason?: undefined } | { path?: undefined; reason: string };
export function withPinnedZig<T>(tarball: string, use: (zig: string, env: Record<string, string>) => T): T;
export function fetchPinnedZig(): Promise<string>;
