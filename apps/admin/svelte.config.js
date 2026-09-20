import adapter from '@sveltejs/adapter-static';

const base = process.env.BIGBROTR_ADMIN_BASE ?? '';
const revision = process.env.BIGBROTR_BUILD_REVISION ?? 'development';
if (revision !== 'development' && !/^[0-9a-f]{40}$/.test(revision)) throw new Error('Invalid build revision');
if (!['', '/control'].includes(base)) throw new Error('BIGBROTR_ADMIN_BASE must be empty or /control');

/** @type {import('@sveltejs/kit').Config} */
export default {
  kit: {
    adapter: adapter({ pages: 'build', assets: 'build', strict: true }),
    paths: { base },
    version: { name: revision, pollInterval: 0 },
    csp: {
      mode: 'hash',
      directives: {
        'default-src': ['none'],
        'script-src': ['self'],
        'style-src': ['self'],
        'connect-src': ['self'],
        'img-src': ['self'],
        'base-uri': ['none'],
        'form-action': ['self'],
        'frame-ancestors': ['none'],
        'object-src': ['none']
      }
    }
  }
};
