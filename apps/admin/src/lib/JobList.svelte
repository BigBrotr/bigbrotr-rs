<script lang="ts">
  import { onDestroy } from 'svelte';
  import { createManagementClient, ManagementFailure } from '@bigbrotr/api-client/management';
  import type { JobObservation, JobListQuery } from '@bigbrotr/api-client/management';
  import { JobPageModel, jobPageFailure, jobStateLabels, utcDayStart, type JobPageView } from './jobs';
  import { utcTime } from './messages';

  const { onSelect, onDenied }: { onSelect: (id: string) => void; onDenied: () => void } = $props();
  let filterState = $state<JobObservation['state'] | ''>('');
  let from = $state('');
  let before = $state('');
  let pageSize = $state<number | undefined>(50);
  let view = $state<JobPageView>({ kind: 'empty' });
  let inputFailure = $state<ManagementFailure>();
  const model = new JobPageModel(createManagementClient(), next => { view = next; }, () => onDenied());
  onDestroy(() => model.dispose());

  function changed() { inputFailure = undefined; model.clear(); }
  function list(event: SubmitEvent) {
    event.preventDefault();
    inputFailure = undefined;
    try {
      const query: JobListQuery = { page_size: pageSize ?? 0 };
      if (filterState) query.state = filterState;
      const first = utcDayStart(from);
      const last = utcDayStart(before);
      if (first !== undefined) query.created_from_us = first;
      if (last !== undefined) query.created_before_us = last;
      void model.start(query);
    } catch {
      model.clear();
      inputFailure = new ManagementFailure('invalid_input');
    }
  }
</script>

<section class="job-list" aria-labelledby="job-list-title">
  <h2 id="job-list-title">Your exports</h2>
  <p>Find jobs started by your account. Select a job to check its latest state.</p>
  <form onsubmit={list} oninput={changed}>
    <div class="job-filters">
      <div class="field">
        <label for="job-state">Job state</label>
        <select id="job-state" bind:value={filterState}>
          <option value="">All states</option>
          {#each Object.entries(jobStateLabels) as [value, label]}<option {value}>{label}</option>{/each}
        </select>
      </div>
      <div class="field">
        <label for="job-page-size">Jobs per page</label>
        <input id="job-page-size" type="number" min="1" max="200" step="1" required bind:value={pageSize} />
      </div>
      <div class="field">
        <label for="job-from">Created on or after (UTC)</label>
        <input id="job-from" type="date" min="0001-01-01" max="9999-12-31" bind:value={from} />
      </div>
      <div class="field">
        <label for="job-before">Created before (UTC)</label>
        <input id="job-before" type="date" min="0001-01-01" max="9999-12-31" bind:value={before} />
      </div>
    </div>
    <button type="submit" class="secondary" disabled={view.kind === 'checking'}><span class="button-label">List my exports</span></button>
    <p class="help">A new listing includes newly created jobs. Changing a filter clears the current page without sending a request.</p>
  </form>
  <div role="status" aria-live="polite">
    {#if view.kind === 'checking'}<p class="progress">Loading your exports…</p>{/if}
    {#if inputFailure || view.failure}
      <p class="notice failure">{jobPageFailure(inputFailure ?? view.failure!)}
        {#if view.failure?.api?.correlation_id}<span class="correlation">Request ID: <code>{view.failure.api.correlation_id}</code></span>{/if}
      </p>
    {/if}
    {#if view.page}
      <p class="help">Observed {utcTime(view.page.observed_at_us)}. Jobs are ordered from newest to oldest; states can change between pages.</p>
      {#if view.page.items.length === 0}
        <p>{view.page.next_cursor ? 'No matching jobs on this page. Continue to check the next page.' : 'No matching jobs on this page. This listing has ended.'}</p>
      {:else}
        <ul class="job-rows">
          {#each view.page.items as job (job.job_id)}
            <li>
              <p class="job-row-state">{jobStateLabels[job.state]}</p>
              <p class="help">Created {utcTime(job.created_at_us)}</p>
              <button type="button" class="text-button job-open" onclick={() => onSelect(job.job_id)} aria-label={`Inspect export ${job.job_id}`}><span class="button-label"><code>{job.job_id}</code></span></button>
            </li>
          {/each}
        </ul>
      {/if}
      {#if !view.page.next_cursor && view.page.items.length > 0}<p class="help">End of this listing.</p>{/if}
    {/if}
  </div>
  {#if view.kind !== 'empty' && !view.failure}
    <button type="button" class="secondary" disabled={view.kind === 'checking' || !view.page?.next_cursor} onclick={() => void model.next()}><span class="button-label">Next page</span></button>
  {/if}
  <p class="help">Listing does not extend your session or confirm that an export file is available.</p>
</section>
