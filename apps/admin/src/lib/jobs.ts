import { ManagementFailure } from '@bigbrotr/api-client/management';
import type { JobObservation, JobListQuery, JobPage, ManagementClient } from '@bigbrotr/api-client/management';

export const jobStateLabels: Record<JobObservation['state'], string> = {
  accepted: 'Accepted', queued: 'Queued', running: 'Running', paused: 'Paused',
  cancel_requested: 'Cancellation requested', succeeded: 'Succeeded', partial: 'Partially completed',
  failed: 'Failed', cancelled: 'Cancelled', skipped: 'Skipped'
};

export type JobView = {
  kind: 'empty' | 'checking' | 'observed' | 'failed';
  observation?: JobObservation;
  failure?: ManagementFailure;
};

/** One explicit lookup, owned by the current session view. Never polls or caches. */
export class JobModel {
  view: JobView = { kind: 'empty' };
  private controller?: AbortController;
  private generation = 0;
  private disposed = false;
  private api: Pick<ManagementClient, 'jobStatus'>;
  private publish: (view: JobView) => void;
  private denied: () => void;
  constructor(
    api: Pick<ManagementClient, 'jobStatus'>,
    publish: (view: JobView) => void,
    denied: () => void
  ) { this.api = api; this.publish = publish; this.denied = denied; }
  private set(view: JobView): void { this.view = view; this.publish(view); }
  async inspect(id: string): Promise<void> {
    if (this.disposed) return;
    this.controller?.abort();
    const generation = ++this.generation;
    const controller = new AbortController();
    this.controller = controller;
    this.set({ kind: 'checking' });
    try {
      const observation = await this.api.jobStatus(id, controller.signal);
      if (this.disposed || generation !== this.generation || controller.signal.aborted) return;
      this.set({ kind: 'observed', observation });
    } catch (error) {
      if (this.disposed || generation !== this.generation || controller.signal.aborted) return;
      const failure = error instanceof ManagementFailure ? error : new ManagementFailure('unavailable');
      this.set({ kind: 'failed', failure });
      if (failure.api?.status === 401) this.denied();
    } finally {
      if (generation === this.generation) this.controller = undefined;
    }
  }
  dispose(): void {
    this.disposed = true;
    this.generation++;
    this.controller?.abort();
    this.controller = undefined;
    this.view = { kind: 'empty' };
  }
}

export type JobPageView = {
  kind: 'empty' | 'checking' | 'observed' | 'failed';
  page?: JobPage;
  failure?: ManagementFailure;
};

/** Only the current page and continuation belong to this session. No page cache. */
export class JobPageModel {
  view: JobPageView = { kind: 'empty' };
  private filters: Omit<JobListQuery, 'cursor'> = {};
  private controller?: AbortController;
  private generation = 0;
  private disposed = false;
  private api: Pick<ManagementClient, 'listJobs'>;
  private publish: (view: JobPageView) => void;
  private denied: () => void;
  constructor(api: Pick<ManagementClient, 'listJobs'>, publish: (view: JobPageView) => void, denied: () => void) {
    this.api = api; this.publish = publish; this.denied = denied;
  }
  private set(view: JobPageView): void { this.view = view; this.publish(view); }
  async start(filters: Omit<JobListQuery, 'cursor'> = {}): Promise<void> {
    if (this.disposed) return;
    const query: JobListQuery = { ...filters };
    delete query.cursor;
    this.filters = query;
    await this.load(this.filters);
  }
  async next(): Promise<void> {
    const cursor = this.view.page?.next_cursor;
    if (this.disposed || this.view.kind !== 'observed' || !cursor) return;
    await this.load({ ...this.filters, cursor });
  }
  private async load(query: JobListQuery): Promise<void> {
    this.controller?.abort();
    const generation = ++this.generation;
    const controller = new AbortController();
    this.controller = controller;
    this.set({ kind: 'checking' });
    try {
      const page = await this.api.listJobs(query, controller.signal);
      if (this.disposed || generation !== this.generation || controller.signal.aborted) return;
      this.set({ kind: 'observed', page });
    } catch (error) {
      if (this.disposed || generation !== this.generation || controller.signal.aborted) return;
      const failure = error instanceof ManagementFailure ? error : new ManagementFailure('unavailable');
      this.set({ kind: 'failed', failure });
      if (failure.api?.status === 401) this.denied();
    } finally {
      if (generation === this.generation) this.controller = undefined;
    }
  }
  clear(): void {
    if (this.disposed) return;
    this.generation++;
    this.controller?.abort();
    this.controller = undefined;
    this.filters = {};
    this.set({ kind: 'empty' });
  }
  dispose(): void { this.clear(); this.disposed = true; }
}

/** A calendar day starts at UTC midnight; never use the browser's local zone. */
export function utcDayStart(day: string): string | undefined {
  if (day === '') return undefined;
  if (!/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(day)) throw new ManagementFailure('invalid_input');
  const date = new Date(`${day}T00:00:00.000Z`);
  if (!Number.isFinite(date.getTime()) || date.toISOString().slice(0, 10) !== day) throw new ManagementFailure('invalid_input');
  return (BigInt(date.getTime()) * 1000n).toString();
}

export function jobPageFailure(failure: ManagementFailure): string {
  if (failure.api?.code === 'cursor_expired') return 'This listing has expired. Select List my exports to start a new listing.';
  if (failure.api?.code === 'cursor_incompatible' || failure.api?.code === 'invalid_cursor') return 'This listing can no longer continue. Check your session and filters, then start a new listing with List my exports.';
  if (failure.api?.status === 403) return 'Your account cannot list jobs. Check your access with the instance owner.';
  if (failure.api?.status === 503) return 'Job listing is unavailable. Ask the instance owner to check its prerequisites, then try again.';
  if (failure.kind === 'invalid_input') return 'Check the filters: use valid UTC dates with the end after the start, and a page size from 1 to 200.';
  if (failure.kind === 'incompatible' || failure.kind === 'response_limit') return 'The panel could not read this page. Check that the panel and instance versions match.';
  if (failure.api?.status === 401 || failure.api?.status === 429 || failure.kind === 'csrf_missing') return jobFailure(failure);
  return 'Jobs could not be listed. Check that the instance is running, then select List my exports to try again.';
}

export function jobFailure(failure: ManagementFailure): string {
  if (failure.api?.status === 404) return 'This job is unavailable to your account. Check its ID and the account that started it.';
  if (failure.api?.status === 401) return 'Your session has ended. Sign in again to inspect your job.';
  if (failure.api?.status === 403) return 'Your account cannot inspect this job. Check your access with the instance owner.';
  if (failure.api?.status === 429) return 'Request capacity was reached. Wait at least one minute before trying again.';
  if (failure.kind === 'invalid_input') return 'Enter the 32-character job ID from your acceptance receipt.';
  if (failure.kind === 'csrf_missing') return 'The browser session token is unavailable. Check your session before trying again.';
  if (failure.kind === 'incompatible' || failure.kind === 'response_limit') return 'The panel could not read this job response. Check that the panel and instance versions match.';
  return 'The job could not be checked. Check that the instance is running, then try again.';
}
