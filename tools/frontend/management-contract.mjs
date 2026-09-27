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
// Resolve the shared read request relative to the owning contract, so export
// cannot acquire a divergent copy of QueryRequest in its generated client.
await output('management.d.ts', '// Generated from contracts/management/v1/openapi.yaml. Do not edit.\n' + astToString(await openapiTS(new URL('../../contracts/management/v1/openapi.yaml', import.meta.url))));
const ajv = new Ajv2020({ strict: true, allErrors: false, code: { source: true, esm: true } });
const definitions = JSON.parse(JSON.stringify(schema.components.schemas).replaceAll('#/components/schemas/', '#/$defs/'));
ajv.addSchema({ $id: 'bigbrotr-private-session', $defs: definitions });
await output('validators.js', '// Generated from contracts/management/v1/openapi.yaml. Do not edit.\n' + standalone(ajv, {
  validateSession: 'bigbrotr-private-session#/$defs/SessionObservation',
  validateError: 'bigbrotr-private-session#/$defs/ApiError',
  validateJob: 'bigbrotr-private-session#/$defs/JobObservation',
  validateJobListQuery: 'bigbrotr-private-session#/$defs/JobListQuery',
  validateJobPage: 'bigbrotr-private-session#/$defs/JobPage',
  validateResumeCommand: 'bigbrotr-private-session#/$defs/ResumeExportCommand',
  validateResumeReceipt: 'bigbrotr-private-session#/$defs/ResumeExportAcceptance'
}) + '\n');
await output('validators.d.ts', `// Generated from contracts/management/v1/openapi.yaml. Do not edit.
import type { components } from './management';
export declare function validateSession(value: unknown): value is components['schemas']['SessionObservation'];
export declare function validateError(value: unknown): value is components['schemas']['ApiError'];
export declare function validateJob(value: unknown): value is components['schemas']['JobObservation'];
export declare function validateJobListQuery(value: unknown): value is components['schemas']['JobListQuery'];
export declare function validateJobPage(value: unknown): value is components['schemas']['JobPage'];
export declare function validateResumeCommand(value: unknown): value is components['schemas']['ResumeExportCommand'];
export declare function validateResumeReceipt(value: unknown): value is components['schemas']['ResumeExportAcceptance'];
`);
