<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { createManagementClient } from '@bigbrotr/api-client/management';
  import type { JobObservation, SessionObservation } from '@bigbrotr/api-client/management';
  import { JobModel, jobFailure, type JobView } from './jobs';
  import { ResumeModel, resumeFailure, type ResumeView } from './resume';
  import { BrowserResumeStore } from './resume-store';
  import { utcTime } from './messages';

  const { session, onDenied }: { session: SessionObservation; onDenied: () => void } = $props();
  let id = $state('');
  let view = $state<JobView>({ kind: 'empty' });
  let resume = $state<ResumeView>({ kind: 'loading' });
  const client = createManagementClient();
  const model = new JobModel(client, next => { view = next; }, () => onDenied());
  // The parent keys this component by the session observation: context cannot
  // change while an old command or local storage operation is still in flight.
  let resumer: ResumeModel | undefined;
  onMount(() => {
    resumer = new ResumeModel(session, client, new BrowserResumeStore(), next => { resume = next; }, () => onDenied());
    void resumer.restore();
  });
  onDestroy(() => { model.dispose(); resumer?.dispose(); });
  const labels: Record<JobObservation['state'], string> = {
    accepted: 'Accepted', queued: 'Queued', running: 'Running', paused: 'Paused',
    cancel_requested: 'Cancellation requested', succeeded: 'Succeeded', partial: 'Partially completed',
    failed: 'Failed', cancelled: 'Cancelled', skipped: 'Skipped'
  };
  function inspect(event: SubmitEvent) { event.preventDefault(); void model.inspect(id); }
</script>

<section class="job-sheet" aria-labelledby="job-title">
  <p class="eyebrow">Work / Export</p>
  <h2 id="job-title">Inspect your job</h2>
  <p>Enter the job ID from your acceptance receipt. You can reconnect here after closing the browser.</p>
  <form onsubmit={inspect}>
    <div class="field">
      <label for="job-id">Job ID</label>
      <input id="job-id" name="job-id" type="text" autocomplete="off" spellcheck="false" autocapitalize="none" minlength="32" maxlength="32" pattern={'[0-9a-f]{32}'} required bind:value={id} aria-describedby="job-help" />
      <p id="job-help" class="help">32 lowercase hexadecimal characters. Only jobs started by your account are shown.</p>
    </div>
    <button type="submit" class="secondary" disabled={view.kind === 'checking'}>{view.kind === 'checking' ? 'Checking job…' : 'Check job'}</button>
  </form>
  <div role="status" aria-live="polite">
    {#if view.kind === 'checking'}<p class="progress">Checking the stored job state…</p>{/if}
    {#if view.failure}
      <p class="notice failure">{jobFailure(view.failure)}
        {#if view.failure.api?.correlation_id}<span class="correlation">Request ID: <code>{view.failure.api.correlation_id}</code></span>{/if}
      </p>
    {/if}
    {#if view.observation}
      {@const job = view.observation}
      <div class="job-observation">
        <p class="job-state">{labels[job.state]}</p>
        <p class="help">Observed {utcTime(job.observed_at_us)}. Check again for changes.</p>
        <dl class="session-facts">
          <div><dt>Job</dt><dd><code>{job.job_id}</code></dd></div>
          <div><dt>Accepted</dt><dd>{utcTime(job.created_at_us)}</dd></div>
          {#if job.completed_at_us}<div><dt>Finished</dt><dd>{utcTime(job.completed_at_us)}</dd></div>{/if}
          <div><dt>Job revision / checkpoint</dt><dd>{job.job_revision} / {job.checkpoint_revision}</dd></div>
        </dl>
        {#if job.reason_code}
          <p>{job.reason_code === 'resource_limit' ? 'A resource limit stopped this attempt.' : job.reason_code === 'execution_refused' ? 'Execution could not proceed under the current permissions or prerequisites.' : 'The job stopped with an operational reason. Ask the instance owner to inspect it.'}</p>
        {/if}
        {#if job.state === 'paused'}
          <p>The job remains paused after restart. Check its limits and prerequisites before resuming. Resume keeps its original selection, destination and limits.</p>
          <button type="button" class="primary" disabled={resume.kind !== 'ready' || resume.attempted === job}
            onclick={() => void resumer?.start(job)}>Resume this export</button>
          {#if resume.attempted === job}<p class="help">Check the job again before starting another resume. Use the saved request below to resolve an uncertain reply.</p>{/if}
        {/if}
        <p class="help">This is the stored state, not a worker health check or a download link. Inspecting a job does not extend your session.</p>
      </div>
    {/if}
  </div>
  <section class="resume-sheet" aria-labelledby="resume-title">
    <h3 id="resume-title">Saved resume request</h3>
    <p class="help">This browser keeps one request for your account so you can recover after closing the tab. Signing out keeps that recovery information.</p>
    <div role="status" aria-live="polite">
      {#if resume.kind !== 'ready'}<p class="progress">{resume.kind === 'loading' ? 'Loading saved request…' : 'Resolving request…'}</p>{/if}
      {#if resume.failure}<p class="notice failure">{resumeFailure(resume.failure)}</p>{/if}
      {#if resume.reviewSaved}<p class="notice">A saved request needs your review. Load the latest saved request before retrying; no different job was submitted automatically.</p>{/if}
      {#if resume.receipt}
        <p class="notice">Resume accepted at {utcTime(resume.receipt.accepted_at_us)} (job revision {resume.receipt.job_revision}). Check the job to see whether execution has started or finished.</p>
      {/if}
      {#if resume.record}
        <dl class="session-facts">
          <div><dt>Saved job</dt><dd><code>{resume.record.job_id}</code></dd></div>
          <div><dt>Request key</dt><dd><code>{resume.record.key}</code></dd></div>
          <div><dt>Saved outcome</dt><dd>{resume.record.outcome === 'pending' ? 'Unconfirmed' : resume.record.outcome === 'accepted' ? 'Accepted' : 'Refused'}</dd></div>
        </dl>
        <div class="actions">
          {#if resume.record.outcome !== 'refused'}
            <button type="button" class="secondary" disabled={resume.kind !== 'ready'} onclick={() => void resumer?.retry()}>Retry saved request</button>
          {/if}
          <button type="button" class="secondary" disabled={view.kind === 'checking'} onclick={() => {
            if (resume.record) { id = resume.record.job_id; void model.inspect(id); }
          }}>Check saved job</button>
          {#if resume.record.outcome !== 'pending'}
            <button type="button" class="text-button" disabled={resume.kind !== 'ready'} onclick={() => void resumer?.forget()}>Forget resolved request</button>
          {/if}
        </div>
        <p class="help">Retry sends the original request unchanged. An acceptance confirms the command, not the job’s completion. Forgetting this browser record changes no job.</p>
      {:else if resume.kind === 'ready' && !resume.failure}<p>No resume request is saved for this account.</p>{/if}
    </div>
    <button type="button" class="text-button" disabled={resume.kind !== 'ready'} onclick={() => void resumer?.restore()}>Load saved request</button>
  </section>
</section>
