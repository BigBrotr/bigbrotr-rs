// One checked-in API contract owns the generated types and runtime validators.
import { readFile, writeFile } from 'node:fs/promises';
import openapiTS, { astToString } from 'openapi-typescript';
import Ajv2020 from 'ajv/dist/2020.js';
import standalone from 'ajv/dist/standalone/index.js';

if (!['', '--check'].includes(process.argv.slice(2).join(' '))) throw new Error('Use --check or no argument');
const check = process.argv.includes('--check');
const schema = JSON.parse(await readFile(new URL('../../contracts/management/v1/openapi.yaml', import.meta.url), 'utf8'));
const base = new URL('../../packages/api-client/src/generated/', import.meta.url);
async function output(name, value) {
  const path = new URL(name, base);
  if (check) {
    if (await readFile(path, 'utf8') !== value) throw new Error(`Generated contract drift: ${name}`);
  } else await writeFile(path, value);
}
await output('management.d.ts', '// Generated from contracts/management/v1/openapi.yaml. Do not edit.\n' + astToString(await openapiTS(schema)));
const ajv = new Ajv2020({ strict: true, allErrors: false, code: { source: true, esm: true } });
const definitions = JSON.parse(JSON.stringify(schema.components.schemas).replaceAll('#/components/schemas/', '#/$defs/'));
ajv.addSchema({ $id: 'bigbrotr-private-session', $defs: definitions });
await output('validators.js', '// Generated from contracts/management/v1/openapi.yaml. Do not edit.\n' + standalone(ajv, {
  validateSession: 'bigbrotr-private-session#/$defs/SessionObservation',
  validateError: 'bigbrotr-private-session#/$defs/ApiError'
}) + '\n');
await output('validators.d.ts', `// Generated from contracts/management/v1/openapi.yaml. Do not edit.
import type { components } from './management';
export declare function validateSession(value: unknown): value is components['schemas']['SessionObservation'];
export declare function validateError(value: unknown): value is components['schemas']['ApiError'];
`);
