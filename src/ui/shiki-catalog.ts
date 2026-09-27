import { createRequire } from "node:module";
import type { DynamicImportLanguageRegistration } from "shiki";

// Render callbacks are synchronous. Resolve optional catalogs on the first
// preview, not while loading the extension (even metadata has a static graph).
const require = createRequire(import.meta.url);
let languages: Record<string, DynamicImportLanguageRegistration> | undefined;
let themeTypes: Map<string, "light" | "dark"> | undefined;

export const shikiLanguages = (): Record<string, DynamicImportLanguageRegistration> =>
  languages ??= {
    ...(require("shiki/langs") as typeof import("shiki/langs")).bundledLanguages,
    bend: () => import("./languages/bend.js"),
  };

export const shikiThemeType = (id: string): "light" | "dark" | undefined => {
  themeTypes ??= new Map((require("shiki/themes") as typeof import("shiki/themes")).bundledThemesInfo.map(theme => [theme.id, theme.type]));
  return themeTypes.get(id);
};
