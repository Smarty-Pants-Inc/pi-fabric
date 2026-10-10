/** Same regex family as Pi 0.87's pi-ai/utils/retry, without importing provider
 * engines into the standalone worker. Account limits and context errors are terminal. */
const nonRetryable = /GoUsageLimitError|FreeUsageLimitError|Monthly usage limit reached|available balance|insufficient_quota|out of budget|quota exceeded|billing|maximum context length|context.{0,20}(?:overflow|exceeded|too long)|too many tokens|prompt is too long|400\b|401\b|403\b/i;
const retryable = /overloaded|currently experiencing high demand|rate.?limit|too many requests|429|5\d\d|service.?unavailable|server.?error|internal.?error|provider.?returned.?error|exceeded request buffer limit while retrying upstream|network.?error|connection.?error|connection.?refused|connection.?lost|other side closed|fetch failed|getaddrinfo|ENOTFOUND|EAI_AGAIN|upstream.?connect|reset before headers|socket hang up|socket connection was closed|timed? out|timeout|terminated|websocket.?closed|websocket.?error|ended without|stream ended before message_stop|stream ended before a terminal response event|http2 request did not get a response|retry delay|you can retry your request|try your request again|please retry your request|ResourceExhausted/i;

export const retryableProviderError = (error: string): boolean => !nonRetryable.test(error) && retryable.test(error);

/** Narrow terminal-report recovery: not generic provider, auth, or quota failures. */
export const interruptedModelStreamError = (error: string): boolean => !nonRetryable.test(error) &&
  /premature(?:ly)? clos(?:e|ed)|stream[^\n]*(?:disconnect|clos(?:e|ed)|ended before|ended without)|server_is_overloaded|other side closed|socket hang up|socket connection was closed/i.test(error);
