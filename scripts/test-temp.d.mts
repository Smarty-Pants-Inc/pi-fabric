export function isolatedTestTemp(prefix: string, capture?: boolean): Record<"TMPDIR" | "TMP" | "TEMP", string>;
export function testTempEnvironment(): Record<"TMPDIR" | "TMP" | "TEMP" | "PI_TEST_TEMP_ROOT", string>;
