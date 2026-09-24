import type { JobObservation, ManagementClient, ResumeExportAcceptance, SessionObservation } from '@bigbrotr/api-client/management';
import { ManagementFailure } from '@bigbrotr/api-client/management';
import { ResumeStorageFailure, sameResumeBody, type ResumeRecord, type ResumeStore } from './resume-store.ts';

export type ResumeView = {
  kind: 'loading' | 'ready' | 'busy';
  record?: ResumeRecord;
  receipt?: ResumeExportAcceptance;
  failure?: ManagementFailure | ResumeStorageFailure;
  attempted?: JobObservation;
  reviewSaved?: boolean;
};

/** Session-owned UI state. The stored intent survives this object's lifetime. */
export class ResumeModel {
  view: ResumeView = { kind: 'loading' };
  private disposed = false;
  private controller?: AbortController;
  private readonly context: Pick<SessionObservation, 'principal_id' | 'config_revision' | 'policy_revision'>;
  private readonly api: Pick<ManagementClient, 'resumeExport'>;
  private readonly store: ResumeStore;
  private readonly publish: (view: ResumeView) => void;
  private readonly denied: () => void;
  constructor(context: ResumeModel['context'], api: ResumeModel['api'], store: ResumeStore,
    publish: ResumeModel['publish'], denied: () => void) {
    this.context = { principal_id: context.principal_id, config_revision: context.config_revision, policy_revision: context.policy_revision };
    this.api = api; this.store = store; this.publish = publish; this.denied = denied;
  }
  private set(patch: Partial<ResumeView>): void { this.view = { ...this.view, ...patch }; this.publish(this.view); }
  private live(signal: AbortSignal): boolean { return !this.disposed && !signal.aborted; }
  private async run(work: (signal: AbortSignal) => Promise<void>): Promise<void> {
    if (this.disposed || this.controller) return;
    const controller = new AbortController();
    this.controller = controller;
    this.set({ kind: 'busy', failure: undefined, reviewSaved: false });
    try { await work(controller.signal); }
    catch (error) {
      if (!this.live(controller.signal)) return;
      const failure = error instanceof ManagementFailure || error instanceof ResumeStorageFailure
        ? error : new ResumeStorageFailure('unavailable');
      this.set({ failure });
      if (failure instanceof ManagementFailure && failure.api?.status === 401) this.denied();
    } finally {
      if (this.live(controller.signal)) { this.controller = undefined; this.set({ kind: 'ready' }); }
    }
  }
  async restore(): Promise<void> {
    await this.run(async signal => {
      const record = await this.store.load(this.context.principal_id, signal);
      if (this.live(signal)) this.set({ record, receipt: undefined });
    });
  }
  async start(job: JobObservation): Promise<void> {
    if (this.view.kind !== 'ready' || job.kind !== 'export' || job.state !== 'paused' || job === this.view.attempted) return;
    await this.run(async signal => {
      const bytes = crypto.getRandomValues(new Uint8Array(16));
      if (bytes.every(value => value === 0)) throw new ResumeStorageFailure('unavailable');
      const candidate: ResumeRecord = {
        format: 'bigbrotr-browser-resume/1', principal_id: this.context.principal_id, job_id: job.job_id,
        key: Array.from(bytes, value => value.toString(16).padStart(2, '0')).join(''), outcome: 'pending',
        command: { expected_job_revision: job.job_revision, expected_config_revision: this.context.config_revision,
          expected_policy_revision: this.context.policy_revision }
      };
      const record = await this.store.reserve(candidate, signal);
      if (!this.live(signal)) return;
      this.set({ record, receipt: undefined, attempted: job });
      if (!sameResumeBody(record, candidate)) { this.set({ reviewSaved: true }); return; }
      await this.send(record, signal);
    });
  }
  async retry(): Promise<void> {
    const record = this.view.record;
    if (!record || record.outcome === 'refused') return;
    await this.run(async signal => { this.set({ receipt: undefined }); await this.send(record, signal); });
  }
  private async send(record: ResumeRecord, signal: AbortSignal): Promise<void> {
    let outcome: 'accepted' | 'refused';
    try {
      const receipt = await this.api.resumeExport(record.job_id, record.key, { ...record.command }, signal);
      if (!this.live(signal)) return;
      this.set({ receipt });
      outcome = 'accepted';
    } catch (error) {
      if (!this.live(signal)) return;
      if (!(error instanceof ManagementFailure) || error.api?.status !== 409) throw error;
      this.set({ failure: error });
      outcome = 'refused';
    }
    // Even after a valid HTTP reply, a failed local commit leaves the original
    // stored identity recoverable. Acceptance is never reported as execution.
    const updated = await this.store.settle(record, outcome, signal);
    if (!this.live(signal)) return;
    if (updated) this.set({ record: { ...record, outcome } });
    else this.set({ reviewSaved: true });
  }
  async forget(): Promise<void> {
    const record = this.view.record;
    if (!record || record.outcome === 'pending') return;
    await this.run(async signal => {
      const removed = await this.store.forget(record.principal_id, record.key, signal);
      if (!this.live(signal)) return;
      const current = removed ? undefined : await this.store.load(this.context.principal_id, signal);
      if (this.live(signal)) this.set({ record: current, receipt: undefined, reviewSaved: !removed });
    });
  }
  dispose(): void {
    this.disposed = true;
    this.controller?.abort();
    this.controller = undefined;
    this.view = { kind: 'loading' };
  }
}

export function resumeFailure(failure: ManagementFailure | ResumeStorageFailure): string {
  if (failure instanceof ResumeStorageFailure) {
    if (failure.kind === 'full') return 'This browser has no room for another account’s saved request. Remove a resolved request from an account you can sign in to, then try again.';
    if (failure.kind === 'invalid') return 'The saved request could not be read safely. Keep this browser data for recovery and ask the instance owner for help.';
    return 'The browser could not save or read this request. Keep any saved request and try loading it again before submitting work.';
  }
  if (failure.api?.status === 409) return 'This request was refused because its key or expected revisions conflict. Refresh your session and check the job before starting another resume.';
  if (failure.api?.status === 401) return 'Your session has ended. Sign in again to recover the saved request.';
  if (failure.api?.status === 403) return 'Your current access does not allow this request. The original request remains saved for recovery.';
  if (failure.api?.status === 429) return 'Request capacity was reached. Wait at least one minute before retrying the saved request.';
  return 'The resume could not be confirmed. It may have been accepted. Retry the saved request to resolve its original outcome.';
}
