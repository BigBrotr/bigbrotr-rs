// Package the production static output. Rust verifies this closed manifest before
// binding; no frontend build runs as a side effect of compiling the Rust binary.
import { createHash } from 'node:crypto';
import { readdir, lstat, readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';

const root = fileURLToPath(new URL('../../apps/admin/build/', import.meta.url));
const base = process.env.BIGBROTR_ADMIN_BASE ?? '';
if (!['', '/control'].includes(base)) throw new Error('Unsupported private panel base');
const files = [];
let total = 0;
async function walk(relative = '') {
  for (const name of (await readdir(join(root, relative))).sort()) {
    if (!/^[A-Za-z0-9_-][A-Za-z0-9_.-]*$/.test(name)) throw new Error('Unexpected static filename');
    const path = relative + name;
    if (path.length > 256) throw new Error('Static path limit');
    const full = join(root, path);
    const metadata = await lstat(full);
    if (metadata.isDirectory()) { await walk(path + '/'); continue; }
    if (!metadata.isFile() || !/\.(html|js|css|json|svg)$/.test(path)) throw new Error('Unsupported static asset');
    if (files.length >= 128 || metadata.size > 2 * 1024 * 1024) throw new Error('Static asset limit');
    const bytes = await readFile(full);
    if (bytes.length !== metadata.size || (total += bytes.length) > 16 * 1024 * 1024) throw new Error('Static bundle limit or concurrent change');
    files.push({ path, bytes: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') });
  }
}
await walk();
if (!files.some(file => file.path === 'index.html')) throw new Error('Missing private entrypoint');
const manifest = JSON.stringify({ format: 'bigbrotr-private-assets/1', base_path: base + '/', files }, null, 2) + '\n';
if (Buffer.byteLength(manifest) > 64 * 1024) throw new Error('Static manifest limit');
await writeFile(join(root, 'manifest.json'), manifest, { flag: 'wx' });
process.stdout.write(`Private static bundle: ${files.length} files, ${total} bytes; manifest SHA256 ${createHash('sha256').update(manifest).digest('hex')}\n`);
