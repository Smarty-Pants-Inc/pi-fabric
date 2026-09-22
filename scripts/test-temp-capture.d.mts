export function captureEnabled(): boolean;
export function captureRecord(phase: string, data?: Record<string, unknown>): void;
export function rootState(root: string): Record<string, unknown>;
export function captureAllocation(root: string): void;
export function captureCleanup(phase: string, root: string, error?: unknown): void;
export function captureBoundary(phase: string): void;
export function beginDriverCapture(): () => void;
export function captureAfterShell(shell: string, completedOutput: string): void;
