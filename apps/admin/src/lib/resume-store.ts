import { isResumeExportCommand } from '@bigbrotr/api-client/management';
import type { ResumeExportCommand } from '@bigbrotr/api-client/management';

export type ResumeRecord = {
  format: 'bigbrotr-browser-resume/1';
  principal_id: string;
  job_id: string;
  key: string;
  command: ResumeExportCommand;
  outcome: 'pending' | 'accepted' | 'refused';
};
export class ResumeStorageFailure extends Error {
  readonly kind: 'unavailable' | 'invalid' | 'full' | 'pending';
  constructor(kind: ResumeStorageFailure['kind']) {
    super(`Saved resume request: ${kind}`);
    this.kind = kind;
  }
}
const DATABASE = 'bigbrotr-private-commands';
const STORE = 'export-resume';
// An open request has no abort API. Keep one native operation per factory in
// this realm, including after the caller's deadline, until its terminal event.
// Other tabs still coordinate their writes through native IDB transactions.
const nativeOperations = new WeakSet<IDBFactory>();
const validId = (value: unknown): value is string => typeof value === 'string'
  && value.length === 32 && /^[0-9a-f]{32}$/.test(value) && !/^0+$/.test(value);

/** Untrusted browser data can never add a credential, destination or response. */
export function decodeResumeRecord(value: unknown, principal: string): ResumeRecord {
  const invalid = () => { throw new ResumeStorageFailure('invalid'); };
  if (!validId(principal) || value === null || typeof value !== 'object' || Array.isArray(value)) return invalid();
  const record = value as Record<string, unknown>;
  if (Object.keys(record).sort().join(',') !== 'command,format,job_id,key,outcome,principal_id'
    || record.format !== 'bigbrotr-browser-resume/1' || record.principal_id !== principal
    || !validId(record.job_id) || !validId(record.key) || !isResumeExportCommand(record.command)
    || (record.outcome !== 'pending' && record.outcome !== 'accepted' && record.outcome !== 'refused')) return invalid();
  const copy: ResumeRecord = {
    format: 'bigbrotr-browser-resume/1', principal_id: principal, job_id: record.job_id,
    key: record.key, command: { ...record.command }, outcome: record.outcome
  };
  // All accepted characters are ASCII, so this also bounds serialized UTF-8.
  if (JSON.stringify(copy).length > 512) return invalid();
  return copy;
}
export function sameResumeBody(a: ResumeRecord, b: ResumeRecord): boolean {
  return a.principal_id === b.principal_id && a.job_id === b.job_id
    && a.command.expected_job_revision === b.command.expected_job_revision
    && a.command.expected_config_revision === b.command.expected_config_revision
    && a.command.expected_policy_revision === b.command.expected_policy_revision;
}
export interface ResumeStore {
  load(principal: string, signal?: AbortSignal): Promise<ResumeRecord | undefined>;
  reserve(candidate: ResumeRecord, signal?: AbortSignal): Promise<ResumeRecord>;
  settle(record: ResumeRecord, outcome: 'accepted' | 'refused', signal?: AbortSignal): Promise<boolean>;
  forget(principal: string, key: string, signal?: AbortSignal): Promise<boolean>;
}

/** Each operation closes its connection; read/write serialization is native to IDB. */
export class BrowserResumeStore implements ResumeStore {
  private readonly factory: IDBFactory | undefined;
  constructor(factory: IDBFactory | undefined = globalThis.indexedDB) { this.factory = factory; }

  private operate<T>(mode: IDBTransactionMode,
    work: (store: IDBObjectStore, result: (value: T) => void, fail: (error: unknown) => void) => void,
    signal?: AbortSignal): Promise<T> {
    const factory = this.factory;
    if (!factory || signal?.aborted || nativeOperations.has(factory)) {
      return Promise.reject(new ResumeStorageFailure('unavailable'));
    }
    nativeOperations.add(factory);
    const release = () => { nativeOperations.delete(factory); };
    return new Promise((resolve, reject) => {
      let finished = false;
      let database: IDBDatabase | undefined;
      let transaction: IDBTransaction | undefined;
      let result: T;
      let hasResult = false;
      const finish = (error?: unknown) => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        signal?.removeEventListener('abort', cancel);
        database?.close();
        if (error) reject(error instanceof ResumeStorageFailure ? error : new ResumeStorageFailure('unavailable'));
        else if (hasResult) resolve(result);
        else reject(new ResumeStorageFailure('invalid'));
      };
      const fail = (error: unknown) => {
        // A commit already in progress can outlive this wait. No HTTP is sent
        // from a rejected reservation; a later load resolves the stored intent.
        try { transaction?.abort(); } catch { /* Already committing/finished. */ }
        finish(error);
      };
      const cancel = () => fail(new ResumeStorageFailure('unavailable'));
      const timer = setTimeout(cancel, 2_000);
      signal?.addEventListener('abort', cancel, { once: true });
      try {
        const open = factory.open(DATABASE, 1);
        open.onblocked = cancel;
        open.onerror = () => { release(); cancel(); };
        open.onupgradeneeded = () => {
          if (finished) { open.transaction?.abort(); return; }
          try { open.result.createObjectStore(STORE, { keyPath: 'principal_id' }); }
          catch (error) { open.transaction?.abort(); fail(error); }
        };
        open.onsuccess = () => {
          database = open.result;
          if (finished) { database.close(); release(); return; }
          database.onversionchange = cancel;
          try {
            if (database.objectStoreNames.length !== 1 || !database.objectStoreNames.contains(STORE)) {
              throw new ResumeStorageFailure('invalid');
            }
            transaction = database.transaction(STORE, mode, { durability: 'strict' });
            transaction.oncomplete = () => { release(); finish(); };
            transaction.onabort = () => { release(); cancel(); };
            transaction.onerror = cancel;
            if (transaction.durability !== 'strict') throw new ResumeStorageFailure('unavailable');
            const store = transaction.objectStore(STORE);
            if (store.keyPath !== 'principal_id' || store.autoIncrement || store.indexNames.length !== 0) {
              throw new ResumeStorageFailure('invalid');
            }
            work(store, value => { result = value; hasResult = true; }, fail);
          } catch (error) { if (!transaction) release(); fail(error); }
        };
      } catch (error) { release(); fail(error); }
    });
  }

  load(principal: string, signal?: AbortSignal): Promise<ResumeRecord | undefined> {
    if (!validId(principal)) return Promise.reject(new ResumeStorageFailure('invalid'));
    return this.operate('readonly', (store, result, fail) => {
      const read = store.get(principal);
      read.onsuccess = () => {
        try { result(read.result === undefined ? undefined : decodeResumeRecord(read.result, principal)); }
        catch (error) { fail(error); }
      };
    }, signal);
  }
  reserve(candidate: ResumeRecord, signal?: AbortSignal): Promise<ResumeRecord> {
    const copy = decodeResumeRecord(candidate, candidate.principal_id);
    if (copy.outcome !== 'pending') return Promise.reject(new ResumeStorageFailure('invalid'));
    return this.operate('readwrite', (store, result, fail) => {
      const write = () => { store.put(copy); result(copy); };
      const read = store.get(copy.principal_id);
      read.onsuccess = () => {
        try {
          if (read.result !== undefined) {
            const prior = decodeResumeRecord(read.result, copy.principal_id);
            // Return the original unresolved identity even for a different
            // proposed job/body. The consumer must present it before retrying.
            if (prior.outcome === 'pending' || (prior.outcome === 'accepted' && sameResumeBody(prior, copy))) {
              result(prior); return;
            }
            write(); return;
          }
          const count = store.count();
          count.onsuccess = () => {
            try {
              if (count.result >= 32) throw new ResumeStorageFailure('full');
              write();
            } catch (error) { fail(error); }
          };
        } catch (error) { fail(error); }
      };
    }, signal);
  }
  settle(record: ResumeRecord, outcome: 'accepted' | 'refused', signal?: AbortSignal): Promise<boolean> {
    const copy = decodeResumeRecord({ ...record, outcome }, record.principal_id);
    return this.operate('readwrite', (store, result, fail) => {
      const read = store.get(copy.principal_id);
      read.onsuccess = () => {
        try {
          const prior = read.result === undefined ? undefined : decodeResumeRecord(read.result, copy.principal_id);
          if (!prior || prior.key !== copy.key || !sameResumeBody(prior, copy)) { result(false); return; }
          store.put(copy); result(true);
        } catch (error) { fail(error); }
      };
    }, signal);
  }
  forget(principal: string, key: string, signal?: AbortSignal): Promise<boolean> {
    if (!validId(principal) || !validId(key)) return Promise.reject(new ResumeStorageFailure('invalid'));
    return this.operate('readwrite', (store, result, fail) => {
      const read = store.get(principal);
      read.onsuccess = () => {
        try {
          const prior = read.result === undefined ? undefined : decodeResumeRecord(read.result, principal);
          if (!prior || prior.key !== key) { result(false); return; }
          if (prior.outcome === 'pending') throw new ResumeStorageFailure('pending');
          store.delete(principal); result(true);
        } catch (error) { fail(error); }
      };
    }, signal);
  }
}
