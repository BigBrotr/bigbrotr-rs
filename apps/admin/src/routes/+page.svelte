<script lang="ts">
  import { onMount, tick } from 'svelte';
  import { createManagementClient } from '@bigbrotr/api-client/management';
  import { SessionModel } from '$lib/session';
  import JobStatus from '$lib/JobStatus.svelte';
  import type { SessionView } from '$lib/session';
  import { messages as m, notice, utcTime } from '$lib/messages';
  import './style.css';

  let view = $state<SessionView>({ kind: 'checking' });
  let origin = $state('');
  let username = $state('');
  let password = $state('');
  let model: SessionModel | undefined;
  let heading = $state<HTMLHeadingElement>();
  let noticeElement = $state<HTMLParagraphElement>();
  const busy = $derived(['checking', 'signing-in', 'signing-out'].includes(view.kind));
  const message = $derived(notice(view));

  onMount(() => {
    origin = window.location.origin;
    document.getElementById('startup-recovery')?.setAttribute('hidden', '');
    // The channel carries only invalidation. Credentials and observations never
    // leave the owning tab through storage, URLs or cross-tab messages.
    const channel = typeof BroadcastChannel === 'undefined' ? undefined : new BroadcastChannel('bigbrotr-private-session-v1');
    const local = new SessionModel(createManagementClient(), next => {
      const previous = view.kind;
      view = next;
      if (previous === 'signing-in' || previous === 'signing-out') {
        void tick().then(() => next.failure ? noticeElement?.focus() : heading?.focus());
      }
    }, () => channel?.postMessage({ format: 'bigbrotr-session-invalidation/1' }));
    model = local;
    if (channel) channel.onmessage = event => {
      if (event.data?.format === 'bigbrotr-session-invalidation/1') {
        password = '';
        local.invalidate('changed');
      }
    };
    const checkExpiry = () => local.checkExpiry();
    // BFCache must not restore a prior private observation after another tab
    // changes its session while this page is frozen.
    const pagehide = () => {
      password = '';
      local.suspend();
    };
    const pageshow = (event: PageTransitionEvent) => { if (event.persisted) local.invalidate('recheck'); else checkExpiry(); };
    document.addEventListener('visibilitychange', checkExpiry);
    window.addEventListener('pageshow', pageshow);
    window.addEventListener('pagehide', pagehide);
    void local.inspect();
    return () => {
      password = '';
      local.dispose();
      channel?.close();
      document.removeEventListener('visibilitychange', checkExpiry);
      window.removeEventListener('pageshow', pageshow);
      window.removeEventListener('pagehide', pagehide);
      model = undefined;
    };
  });

  function signIn(event: SubmitEvent) {
    event.preventDefault();
    if (!model || busy) return;
    const input = { username, password };
    password = '';
    void model.login(input);
  }
  function signOut() { password = ''; void model?.logout(); }
</script>

<svelte:head>
  <title>{m.appTitle}</title>
  <meta name="robots" content="noindex,nofollow" />
</svelte:head>

<a class="skip" href="#session">{m.skip}</a>
<div class="shell">
  <header class="spine">
    <div class="identity">
      <p class="wordmark">BigBrotr<span class="wordmark-rule" aria-hidden="true"></span></p>
      <p class="product-line">Verifiable Nostr archive<br />and observatory</p>
    </div>
    <div class="instance">
      <p class="label">{m.privateAdministration}</p>
      <p class="instance-label">{m.instance}</p>
      <p class="origin">{origin || 'Local private address'}</p>
    </div>
    <p class="build-note">{m.initialScope}</p>
  </header>

  <main id="session" class="workspace">
    <div class="session-sheet">
      <p class="eyebrow">{view.session ? 'Account / Session' : 'Account / Sign in'}</p>
      <h1 bind:this={heading} tabindex="-1">{view.session ? m.sessionTitle : m.signInTitle}</h1>
      <p class="intro">{view.session ? m.sessionHelp : m.signInHelp}</p>
      <noscript><p class="notice">{m.scriptRequired}</p></noscript>

      {#if message}
        <p bind:this={noticeElement} class="notice" class:failure={Boolean(view.failure)} role="status" tabindex="-1" id="session-notice">
          {message}
          {#if view.failure?.api?.correlation_id}
            <span class="correlation">{m.requestId}: <code>{view.failure.api.correlation_id}</code></span>
          {/if}
        </p>
      {/if}
      <p class="progress" role="status" aria-live="polite">
        {view.kind === 'checking' ? m.checking : view.kind === 'signing-in' ? m.signingIn : view.kind === 'signing-out' ? m.signingOut : ''}
      </p>

      {#if view.session}
        <dl class="session-facts">
          <div><dt>{m.principal}</dt><dd><code>{view.session.principal_id}</code></dd></div>
          <div><dt>{m.idleExpiry}</dt><dd>{utcTime(view.session.idle_expires_at_us)}</dd></div>
          <div><dt>{m.absoluteExpiry}</dt><dd>{utcTime(view.session.expires_at_us)}</dd></div>
        </dl>
        <section class="permissions" aria-labelledby="permissions-title">
          <h2 id="permissions-title">{m.actions}</h2>
          {#if view.session.stored_actions.length}
            <ul>{#each view.session.stored_actions as action}<li><code>{action}</code></li>{/each}</ul>
          {:else}<p>{m.noActions}</p>{/if}
          <p class="help">{m.actionsHelp}</p>
        </section>
        <div class="actions">
          <button type="button" class="primary" onclick={signOut}>{m.signOut}</button>
          <button type="button" class="secondary" onclick={() => void model?.inspect()}>{m.refresh}</button>
        </div>
        <p class="help">{m.refreshHelp}</p>
        {#key view.session}
          <JobStatus onDenied={() => model?.invalidate('expired')} />
        {/key}
      {:else}
        <form method="POST" onsubmit={signIn} aria-describedby={message ? 'session-notice' : undefined}>
          <div class="field">
            <label for="username">{m.username}</label>
            <input id="username" name="username" autocomplete="username" autocapitalize="none" spellcheck="false" maxlength="64" required bind:value={username} disabled={busy} aria-describedby={message ? "username-help session-notice" : "username-help"} />
            <p id="username-help" class="help">{m.usernameHelp}</p>
          </div>
          <div class="field">
            <label for="password">{m.password}</label>
            <input id="password" name="password" type="password" autocomplete="current-password" maxlength="1024" required bind:value={password} disabled={busy} aria-describedby={message ? "session-notice" : undefined} />
          </div>
          <button class="primary" type="submit" disabled={busy}>{view.kind === 'signing-in' ? m.signingIn : m.signIn}</button>
        </form>
        {#if !busy && (view.failedOperation || view.notice === 'changed' || view.notice === 'recheck')}
          <div class="recovery-actions">
            {#if view.failedOperation === 'logout'}
              <button type="button" class="secondary" onclick={signOut}>{m.confirmSignOut}</button>
            {/if}
            <button type="button" class="text-button" onclick={() => void model?.inspect()}>{m.checkSession}</button>
          </div>
        {/if}
      {/if}
    </div>
  </main>
</div>
