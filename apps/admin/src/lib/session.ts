import { ManagementFailure, sessionDeadlineMs } from '@bigbrotr/api-client/management';
import type { LoginInput, ManagementClient, SessionObservation } from '@bigbrotr/api-client/management';

type Operation = 'inspect' | 'login' | 'logout';
type Notice = 'signed-out' | 'changed' | 'expired' | 'recheck';
export type SessionView = {
  kind: 'checking' | 'signing-in' | 'signing-out' | 'signed-in' | 'signed-out';
  session?: SessionObservation;
  notice?: Notice;
  failure?: ManagementFailure;
  failedOperation?: Operation;
};

/** One in-memory observation. No cookie, password, storage cache or background refresh. */
export class SessionModel {
  view: SessionView = { kind: 'checking' };
  private readonly api: ManagementClient;
  private readonly publish: (view: SessionView) => void;
  private readonly broadcast: () => void;
  private generation = 0;
  private controller?: AbortController;
  private expiry?: ReturnType<typeof setTimeout>;
  private wallDeadline = 0;
  private monotonicDeadline = 0;
  private disposed = false;
  private pendingMutations = 0;

  constructor(api: ManagementClient, publish: (view: SessionView) => void, broadcast: () => void) {
    this.api = api;
    this.publish = publish;
    this.broadcast = broadcast;
  }
  inspect(): Promise<void> { return this.run('inspect', signal => this.api.inspect(signal)); }
  login(input: LoginInput): Promise<void> { return this.run('login', signal => this.api.login(input, signal)); }
  logout(): Promise<void> { return this.run('logout', signal => this.api.logout(signal)); }

  invalidate(notice: Notice): void {
    if (this.disposed) return;
    this.cancel();
    this.set({ kind: 'signed-out', notice });
  }
  /** Visibility/pageshow can check expiry without touching server activity. */
  checkExpiry(): void {
    if (this.disposed || this.view.kind !== 'signed-in') return;
    clearTimeout(this.expiry);
    const remaining = Math.min(this.wallDeadline - Date.now(), this.monotonicDeadline - performance.now());
    if (remaining <= 0) this.invalidate('expired');
    else this.expiry = setTimeout(() => this.checkExpiry(), Math.min(remaining, 2_147_483_647));
  }
  suspend(): void {
    if (this.disposed) return;
    // An idle page going away cannot change cookies in another tab.
    if (this.pendingMutations > 0) this.broadcast();
    this.invalidate('recheck');
  }
  dispose(): void {
    if (this.disposed) return;
    // The caller closes its channel after this method. Headers may already
    // have changed cookies even though the request body never completed.
    if (this.pendingMutations > 0) this.broadcast();
    this.cancel();
    this.disposed = true;
    this.view = { kind: 'signed-out' };
  }
  private cancel(): void {
    this.generation++;
    this.controller?.abort();
    this.controller = undefined;
    clearTimeout(this.expiry);
    this.expiry = undefined;
  }
  private set(view: SessionView): void { this.view = view; this.publish(view); }
  private async run(operation: Operation, execute: (signal: AbortSignal) => Promise<SessionObservation | void>): Promise<void> {
    if (this.disposed) return;
    this.cancel();
    const generation = this.generation;
    const controller = new AbortController();
    this.controller = controller;
    this.set({ kind: operation === 'inspect' ? 'checking' : operation === 'login' ? 'signing-in' : 'signing-out' });
    if (operation !== 'inspect') { this.pendingMutations++; this.broadcast(); }
    try {
      const session = await execute(controller.signal);
      // Abort is advisory once a response has been queued. Generation is the
      // independent guard against logout, cross-tab invalidation and teardown.
      if (this.disposed || generation !== this.generation || controller.signal.aborted) return;
      if (!session) { this.set({ kind: 'signed-out', notice: 'signed-out' }); return; }
      const deadline = sessionDeadlineMs(session);
      if (deadline === undefined) throw new ManagementFailure('incompatible');
      this.wallDeadline = deadline;
      this.monotonicDeadline = performance.now() + Math.max(0, deadline - Date.now());
      this.set({ kind: 'signed-in', session });
      this.checkExpiry();
    } catch (error) {
      if (this.disposed || generation !== this.generation || controller.signal.aborted) return;
      const failure = error instanceof ManagementFailure ? error : new ManagementFailure('unavailable');
      // A missing initial cookie is ordinary signed-out state. Every other
      // failed observation forgets prior private data, including policy changes.
      this.set(operation === 'inspect' && failure.api?.status === 401
        ? { kind: 'signed-out' }
        : { kind: 'signed-out', failure, failedOperation: operation });
    } finally {
      // Cookies may already have changed before a response body fails. A tab
      // that inspected during this mutation must forget that earlier identity
      // even when completion is unknown or this request was superseded.
      if (operation !== 'inspect') {
        this.pendingMutations--;
        if (!this.disposed) this.broadcast();
      }
      if (generation === this.generation) this.controller = undefined;
    }
  }
}
