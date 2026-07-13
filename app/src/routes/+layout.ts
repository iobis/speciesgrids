// This file tells SvelteKit to prerender all routes at build time.
// Required for adapter-static (GitHub Pages) — without it, no index.html
// is generated because SvelteKit doesn't know which pages to render.
export const prerender = true;

// Note: do NOT set ssr = false here. The SvelteKit docs warn that doing so
// causes prerendering to save an empty shell page instead of real HTML.
// Browser-only code (DuckDB, MapLibre, Deck.gl) is already safely guarded
// with `if (!browser)` checks and `onMount()` calls throughout the app.
export const ssr = false;

// 'always' generates docs/index.html instead of docs.html.
// GitHub Pages serves /docs/ correctly from docs/index.html,
// whereas docs.html requires the 404.html fallback to catch /docs requests.
export const trailingSlash = 'always';