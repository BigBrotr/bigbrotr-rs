// Independent research encoder: RFC 8949 core deterministic restricted profile.
// Deliberately separate from the Python probe; no product code/dependencies.
import fs from 'node:fs';
import crypto from 'node:crypto';

function argument(major, number) {
  const n = BigInt(number);
  if (n < 0n || n > 18446744073709551615n) throw Error('integer range');
  const b = Buffer.alloc(9);
  if (n < 24n) return Buffer.from([major * 32 + Number(n)]);
  let width, additional;
  if (n <= 255n) [width, additional] = [1, 24];
  else if (n <= 65535n) [width, additional] = [2, 25];
  else if (n <= 4294967295n) [width, additional] = [4, 26];
  else [width, additional] = [8, 27];
  b[0] = major * 32 + additional;
  for (let j = 0; j < width; j++) b[width - j] = Number((n >> BigInt(8 * j)) & 255n);
  return b.subarray(0, width + 1);
}

function encode(v) {
  if (v === null) return Buffer.from([246]);
  if (v === false || v === true) return Buffer.from([v ? 245 : 244]);
  if (Array.isArray(v)) return Buffer.concat([argument(4, v.length), ...v.map(encode)]);
  if (typeof v === 'object' && '$int' in v) {
    const n = BigInt(v.$int);
    if (n < -9223372036854775808n) throw Error('signed range');
    return n >= 0n ? argument(0, n) : argument(1, -1n - n);
  }
  if (typeof v === 'object' && '$bytes' in v) {
    const b = Buffer.from(v.$bytes, 'hex');
    if (b.toString('hex') !== v.$bytes) throw Error('bad bytes');
    return Buffer.concat([argument(2, b.length), b]);
  }
  if (typeof v === 'number') {
    if (!Number.isSafeInteger(v)) throw Error('unsafe number');
    return v >= 0 ? argument(0, v) : argument(1, -1 - v);
  }
  if (typeof v === 'string') {
    const b = Buffer.from(v, 'utf8');
    if (b.toString('utf8') !== v) throw Error('invalid Unicode');
    return Buffer.concat([argument(3, b.length), b]);
  }
  if (typeof v === 'object') {
    const pairs = Object.entries(v).map(([k, value]) => [encode(k), encode(value)]);
    pairs.sort(([a], [b]) => Buffer.compare(a, b));
    return Buffer.concat([argument(5, pairs.length), ...pairs.flat()]);
  }
  throw Error('unsupported');
}

const vectors = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const output = vectors.vectors.map(v => {
  const payload = v.mode === 'raw' ? Buffer.from(v.raw_hex, 'hex') : encode(v.value);
  const prefix = v.domain ? Buffer.from(`bigbrotr/${v.domain}/1\0`, 'ascii') : Buffer.alloc(0);
  const bytes = Buffer.concat([prefix, payload]);
  return {name: v.name, preimage_hex: bytes.toString('hex'),
    sha256: crypto.createHash('sha256').update(bytes).digest('hex')};
});
process.stdout.write(JSON.stringify(output));
