import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
export default defineConfig({
  plugins: [sveltekit()],
  // Also suppress speculative JS dependencies for client-side dynamic imports.
  // CSS dependencies remain managed by Vite; see the prerender hook's invariant.
  build: { modulePreload: false }
});
