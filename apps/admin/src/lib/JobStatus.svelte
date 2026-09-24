<script lang="ts">
  import { onDestroy } from 'svelte';
  import { createManagementClient } from '@bigbrotr/api-client/management';
  import type { JobObservation } from '@bigbrotr/api-client/management';
  import { JobModel, jobFailure, type JobView } from './jobs';
  import { utcTime } from './messages';

  const { onDenied }: { onDenied: () => void } = $props();
  let id = $state('');
  let view = $state<JobView>({ kind: 'empty' });
  const model = new JobModel(createManagementClient(), next => { view = next; }, () => onDenied());
  onDestroy(() => model.dispose());
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
        {#if job.state === 'paused'}<p>The job remains paused after restart. Check its limits and prerequisites before resuming.</p>{/if}
        <p class="help">This is the stored state, not a worker health check or a download link. Inspecting a job does not extend your session.</p>
      </div>
    {/if}
  </div>
</section>
