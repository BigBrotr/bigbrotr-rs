import type { Handle } from '@sveltejs/kit';

// Runs while prerendering the static bundle; there is no deployed Node server.
// A failed WebKit modulepreload can survive reload (WebKit bug 270357). Ordinary
// imports keep the existing refresh path usable after the asset is restored.
export const handle: Handle = ({ event, resolve }) =>
  resolve(event, { preload: ({ type }) => type === 'css' });
