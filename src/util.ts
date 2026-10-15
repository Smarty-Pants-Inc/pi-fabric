import { detachedPreview } from "./core/preview.js";

export const countNewlines = (value: string): number => {
  let count = 0;
  for (let index = 0; index < value.length; index++) {
    if (value.charCodeAt(index) === 10) count++;
  }
  return count;
};

export const truncateMiddle = (value: string, maxChars: number): string => {
  if (value.length <= maxChars) return detachedPreview(value, value.length);
  const marker = `\n\n... ${value.length - maxChars} characters omitted by Pi Fabric ...\n\n`;
  const available = Math.max(0, maxChars - marker.length);
  const head = Math.ceil(available / 2);
  const tail = Math.floor(available / 2);
  return `${detachedPreview(value, head)}${marker}${detachedPreview(value.slice(value.length - tail), tail)}`;
};
