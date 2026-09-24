import { ManagementFailure } from '@bigbrotr/api-client/management';
import type { JobObservation, ManagementClient } from '@bigbrotr/api-client/management';

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
