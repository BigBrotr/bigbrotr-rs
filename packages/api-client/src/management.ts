import type { components } from './generated/management';
import { validateError, validateSession, validateJob, validateResumeCommand, validateResumeReceipt } from './generated/validators.js';

export type SessionObservation = components['schemas']['SessionObservation'];
export type LoginInput = components['schemas']['LoginInput'];
export type JobObservation = components['schemas']['JobObservation'];
export type ResumeExportCommand = components['schemas']['ResumeExportCommand'];
export type ResumeExportAcceptance = components['schemas']['ResumeExportAcceptance'];
type ApiError = components['schemas']['ApiError']['error'];
type FailureKind = 'api' | 'cancelled' | 'unavailable' | 'incompatible' | 'response_limit' | 'csrf_missing' | 'invalid_input';

// Never retain an arbitrary fetch exception, response body or credential in an error.
export class ManagementFailure extends Error {
  readonly kind: FailureKind;
  readonly api?: ApiError;
  readonly retryAfterSeconds?: number;
  constructor(kind: FailureKind, api?: ApiError, retryAfterSeconds?: number) {
    super(`Private management request failed: ${kind}`);
    this.name = 'ManagementFailure';
    this.kind = kind;
    this.api = api;
    this.retryAfterSeconds = retryAfterSeconds;
  }
}

const RESPONSE_BYTES = 16 * 1024;
const REQUEST_MS = 6_000;
const U64_MAX = 18446744073709551615n;
const I64_MAX = 9223372036854775807n;

function observation(value: unknown): SessionObservation {
  if (!validateSession(value)) throw new ManagementFailure('incompatible');
  for (const key of ['credential_revision', 'grant_revision', 'config_revision', 'policy_revision'] as const) {
    if (BigInt(value[key]) > U64_MAX) throw new ManagementFailure('incompatible');
  }
  const absolute = BigInt(value.expires_at_us);
  const idle = BigInt(value.idle_expires_at_us);
  if (absolute > I64_MAX || idle > absolute) throw new ManagementFailure('incompatible');
  return value;
}

function jobObservation(value: unknown, expected: string): JobObservation {
  if (!validateJob(value) || value.job_id !== expected) throw new ManagementFailure('incompatible');
  for (const key of ['job_revision', 'checkpoint_revision'] as const) {
    if (BigInt(value[key]) > U64_MAX) throw new ManagementFailure('incompatible');
  }
  for (const key of ['created_at_us', 'completed_at_us', 'observed_at_us'] as const) {
    const time = value[key];
    if (time !== null && (BigInt(time) > I64_MAX || BigInt(time) < -I64_MAX - 1n)) throw new ManagementFailure('incompatible');
  }
  const terminal = ['succeeded', 'partial', 'failed', 'cancelled', 'skipped'].includes(value.state);
  if (terminal !== (value.completed_at_us !== null)
    || (['paused', 'cancel_requested', 'partial', 'failed', 'cancelled', 'skipped'].includes(value.state) && value.reason_code === null)) {
    throw new ManagementFailure('incompatible');
  }
  return value;
}

/** Floor to milliseconds expires the display conservatively; never round a u64. */
export function sessionDeadlineMs(value: SessionObservation): number | undefined {
  const deadline = BigInt(value.idle_expires_at_us) / 1000n;
  // JavaScript Date's documented range is narrower than the wire's i64 range.
  return deadline <= 8_640_000_000_000_000n ? Number(deadline) : undefined;
}

function csrfCookie(cookie: string): string {
  if (cookie.length > 8192) throw new ManagementFailure('csrf_missing');
  let token: string | undefined;
  for (const pair of cookie.split(';')) {
    const position = pair.indexOf('=');
    if (position < 0 || pair.slice(0, position).trim() !== 'BigBrotrLocalCsrf') continue;
    const value = pair.slice(position + 1);
    if (token !== undefined || !/^[0-9a-f]{64}$/.test(value)) throw new ManagementFailure('csrf_missing');
    token = value;
  }
  if (token === undefined) throw new ManagementFailure('csrf_missing');
  return token;
}

function loginInput(value: LoginInput): string {
  if (!/^[a-z0-9._-]{1,64}$/.test(value.username) || value.password.length > 1024) {
    throw new ManagementFailure('invalid_input');
  }
  // Rust counts Unicode scalar values and rejects Cc characters; lone UTF-16
  // surrogates must not be silently rewritten by UTF-8 encoding in the browser.
  const scalars = [...value.password];
  if (scalars.length < 12 || /[\p{Cc}\uD800-\uDFFF]/u.test(value.password)
    || new TextEncoder().encode(value.password).length > 1024) {
    throw new ManagementFailure('invalid_input');
  }
  return JSON.stringify({ username: value.username, password: value.password });
}

async function boundedJson(response: Response, signal: AbortSignal): Promise<unknown> {
  const media = response.headers.get('content-type');
  if (!media || !/^application\/json(?:;\s*charset=utf-8)?$/i.test(media) || !response.body) {
    await response.body?.cancel();
    throw new ManagementFailure('incompatible');
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder('utf-8', { fatal: true });
  let bytes = 0;
  let text = '';
  let finished = false;
  // Cancels a stalled reader too. The next signal check prevents a cancelled
  // stream's synthetic EOF from being accepted as a complete response.
  const abort = () => { void reader.cancel().catch(() => {}); };
  signal.addEventListener('abort', abort, { once: true });
  try {
    for (;;) {
      signal.throwIfAborted();
      const next = await reader.read();
      signal.throwIfAborted();
      if (next.done) { finished = true; break; }
      bytes += next.value.byteLength;
      if (bytes > RESPONSE_BYTES) throw new ManagementFailure('response_limit');
      text += decoder.decode(next.value, { stream: true });
    }
    text += decoder.decode();
    return JSON.parse(text) as unknown;
  } catch (error) {
    if (error instanceof ManagementFailure || signal.aborted) throw error;
    throw new ManagementFailure('incompatible');
  } finally {
    signal.removeEventListener('abort', abort);
    if (!finished) await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

/** Fixed paths at the document's private origin; no configurable credential destination. */
export function createManagementClient(environment: {
  fetch?: typeof globalThis.fetch;
  cookie?: () => string;
} = {}) {
  const fetch = environment.fetch ?? globalThis.fetch.bind(globalThis);
  const cookie = environment.cookie ?? (() => document.cookie);
  async function request<T>(path: string, method: 'GET' | 'POST' | 'DELETE', success: number | readonly number[],
    decode: (value: unknown, status: number) => T, signal?: AbortSignal, body?: string, csrf?: string, key?: string): Promise<T | undefined> {
    const controller = new AbortController();
    const cancel = () => controller.abort();
    let timedOut = false;
    signal?.addEventListener('abort', cancel, { once: true });
    if (signal?.aborted) cancel();
    const timer = setTimeout(() => { timedOut = true; cancel(); }, REQUEST_MS);
    try {
      controller.signal.throwIfAborted();
      const headers: Record<string, string> = { Accept: 'application/json' };
      if (body !== undefined) headers['Content-Type'] = 'application/json';
      if (csrf !== undefined) headers['X-CSRF-Token'] = csrf;
      if (key !== undefined) headers['Idempotency-Key'] = key;
      const response = await fetch(path, {
        method, body, headers, signal: controller.signal,
        credentials: 'same-origin', mode: 'same-origin', redirect: 'error', cache: 'no-store'
      });
      if (controller.signal.aborted) { await response.body?.cancel(); controller.signal.throwIfAborted(); }
      if (response.status === 204 && success === 204) return undefined;
      const accepted = typeof success === 'number' ? response.status === success : success.includes(response.status);
      if (!accepted && ![400, 401, 403, 404, 409, 413, 415, 429, 503].includes(response.status)) {
        await response.body?.cancel();
        throw new ManagementFailure('incompatible');
      }
      const value = await boundedJson(response, controller.signal);
      controller.signal.throwIfAborted();
      if (accepted) return decode(value, response.status);
      if (!validateError(value) || value.error.status !== response.status
        || (response.status === 429 && response.headers.get('retry-after') !== '60')) {
        throw new ManagementFailure('incompatible');
      }
      throw new ManagementFailure('api', value.error, response.status === 429 ? 60 : undefined);
    } catch (error) {
      if (controller.signal.aborted) throw new ManagementFailure(timedOut ? 'unavailable' : 'cancelled');
      if (error instanceof ManagementFailure) throw error;
      throw new ManagementFailure('unavailable');
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener('abort', cancel);
    }
  }
  return {
    async login(input: LoginInput, signal?: AbortSignal): Promise<SessionObservation> {
      return (await request('/management/v1/sessions', 'POST', 201, observation, signal, loginInput(input)))!;
    },
    async inspect(signal?: AbortSignal): Promise<SessionObservation> {
      return (await request('/management/v1/me', 'GET', 200, observation, signal))!;
    },
    async logout(signal?: AbortSignal): Promise<void> {
      await request('/management/v1/sessions/current', 'DELETE', 204, () => undefined, signal, undefined, csrfCookie(cookie()));
    },
    async jobStatus(id: string, signal?: AbortSignal): Promise<JobObservation> {
      if (!/^[0-9a-f]{32}$/.test(id) || /^0+$/.test(id)) throw new ManagementFailure('invalid_input');
      return (await request(`/management/v1/jobs/${id}`, 'GET', 200, value => jobObservation(value, id), signal, undefined, csrfCookie(cookie())))!;
    },
    async resumeExport(id: string, key: string, command: ResumeExportCommand, signal?: AbortSignal): Promise<ResumeExportAcceptance> {
      for (const value of [id, key]) {
        if (!/^[0-9a-f]{32}$/.test(value) || /^0+$/.test(value)) throw new ManagementFailure('invalid_input');
      }
      if (!validateResumeCommand(command) || Object.values(command).some(value => BigInt(value) > U64_MAX)) {
        throw new ManagementFailure('invalid_input');
      }
      // Snapshot before the first await: later caller mutation must not change
      // the receipt identity compared against the bytes already dispatched.
      const revision = BigInt(command.expected_job_revision) + 1n;
      const body = JSON.stringify(command);
      return (await request(`/management/v1/jobs/${id}/resume`, 'POST', [200, 202], (value, status) => {
        if (!validateResumeReceipt(value) || value.job_id !== id || value.command_key !== key
          || BigInt(value.job_revision) > U64_MAX || BigInt(value.job_revision) !== revision
          || BigInt(value.accepted_at_us) > I64_MAX || value.created !== (status === 202)) {
          throw new ManagementFailure('incompatible');
        }
        return value;
      }, signal, body, csrfCookie(cookie()), key))!;
    }
  };
}
export type ManagementClient = ReturnType<typeof createManagementClient>;
