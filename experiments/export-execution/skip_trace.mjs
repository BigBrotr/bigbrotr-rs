// Diagnostic fixture only: retain native focus transitions without input values.
// The unknown post-lookup starting point and both controlled starts are separate
// outcomes. A successful controlled start never erases the first failed path.
import assert from 'node:assert/strict';
import { writeFile, rename } from 'node:fs/promises';
import { setTimeout as sleep } from 'node:timers/promises';

export async function traceSkipLink(page, directory, lookup) {
  const report = { format: 'bigbrotr-skip-trace/1', status: 'INCOMPLETE', samples: [], trials: [] };
  const entryUrl = page.url();
  assert.equal(new URL(entryUrl).hash, '');
  // One writer owns the fresh case directory. Atomic replacement preserves the
  // last complete observation if this process is killed during the next write;
  // this is not an fsync or power-loss durability claim.
  const save = async () => {
    await writeFile(directory + '/skip-trace.tmp', JSON.stringify(report, null, 2) + '\n');
    await rename(directory + '/skip-trace.tmp', directory + '/skip-trace.json');
  };
  await page.evaluate(() => {
    const labels = [
      ['skip', '.skip'], ['job-id', '#job-id'], ['heading', 'h1'],
      ['session', '#session'], ['body', 'body'], ['html', 'html']
    ];
    const buttons = new Map([
      ['Sign out', 'sign-out'], ['Refresh session', 'refresh-session'],
      ['Check job', 'check-job'], ['Checking job…', 'check-job'],
      ['Resume this export', 'resume-export'], ['Retry saved request', 'retry-saved'],
      ['Check saved job', 'check-saved'], ['Forget resolved request', 'forget-saved'],
      ['Load saved request', 'load-saved']
    ]);
    const identify = element => {
      if (!(element instanceof Element)) return { label: 'none' };
      const label = labels.find(([, selector]) => element.matches(selector))?.[0]
        ?? (element.matches('button') ? buttons.get(element.textContent.trim()) : undefined) ?? 'other';
      return { label, tag: element.tagName, tabIndex: element.tabIndex ?? null,
        disabled: element instanceof HTMLButtonElement || element instanceof HTMLInputElement ? element.disabled : null };
    };
    const nodes = new WeakMap(); let nextNode = 0;
    const nodeId = node => {
      if (!node) return null;
      if (!nodes.has(node)) nodes.set(node, ++nextNode);
      return nodes.get(node);
    };
    const button = document.getElementById('job-id').closest('form').querySelector('button');
    const buttonState = () => {
      const current = document.getElementById('job-id').closest('form').querySelector('button');
      return { node: nodeId(current), original: current === button, disabled: current.disabled,
        children: [...current.childNodes].map(node => ({ node: nodeId(node), type: node.nodeType })) };
    };
    const state = { events: [], overflow: false, identify, buttonState };
    const append = item => {
      if (state.events.length === 256) { state.overflow = true; return; }
      state.events.push(item);
    };
    const timers = new Set();
    const listen = event => {
      if (event instanceof KeyboardEvent && !['Tab', 'Enter'].includes(event.key)) return;
      const item = { type: event.type, target: identify(event.target), active: identify(document.activeElement),
        trusted: event.isTrusted, monotonicMs: performance.now(), defaultPreventedAtCapture: event.defaultPrevented };
      if (event instanceof KeyboardEvent) {
        Object.assign(item, { key: event.key, shift: event.shiftKey,
          alt: event.altKey, control: event.ctrlKey, meta: event.metaKey });
        // Observe a later task, without pretending capture phase is the final
        // cancellation state. It may be drained by a subsequent sample.
        const timer = setTimeout(() => {
          timers.delete(timer);
          append({ type: 'keyboard-later-task', phase: event.type, key: event.key,
            defaultPrevented: event.defaultPrevented, monotonicMs: performance.now(),
            active: identify(document.activeElement) });
        }, 0);
        timers.add(timer);
      }
      append(item);
    };
    const observer = new MutationObserver(records => {
      for (const record of records) append({ type: 'button-mutation', mutation: record.type,
        node: nodeId(record.target), oldDisabledPresent: record.type === 'attributes' ? record.oldValue !== null : null,
        button: buttonState(), monotonicMs: performance.now() });
    });
    observer.observe(button, { attributes: true, attributeFilter: ['disabled'], attributeOldValue: true,
      childList: true, characterData: true, subtree: true });
    for (const type of ['keydown', 'keyup', 'focusin', 'focusout']) document.addEventListener(type, listen, true);
    for (const type of ['popstate', 'hashchange']) window.addEventListener(type, listen);
    state.dispose = () => {
      observer.disconnect();
      for (const timer of timers) clearTimeout(timer);
      for (const type of ['keydown', 'keyup', 'focusin', 'focusout']) document.removeEventListener(type, listen, true);
      for (const type of ['popstate', 'hashchange']) window.removeEventListener(type, listen);
    };
    window.__bigbrotrSkipTrace = state;
  });
  async function sample(stage) {
    const value = await page.evaluate(() => {
      const state = window.__bigbrotrSkipTrace, link = document.querySelector('.skip');
      const rect = link.getBoundingClientRect(), style = getComputedStyle(link);
      const input = document.getElementById('job-id')?.getBoundingClientRect();
      const viewport = visualViewport;
      return { active: state.identify(document.activeElement), documentFocused: document.hasFocus(), monotonicMs: performance.now(),
        button: state.buttonState(),
        hash: location.hash, events: state.events.splice(0), overflow: state.overflow,
        inputBox: input ? { top: input.top, bottom: input.bottom, left: input.left, right: input.right } : null,
        scroll: { x: scrollX, y: scrollY }, viewport: { width: innerWidth, height: innerHeight },
        visualViewport: viewport ? { width: viewport.width, height: viewport.height,
          offsetLeft: viewport.offsetLeft, offsetTop: viewport.offsetTop, scale: viewport.scale } : null,
        skip: { focused: document.activeElement === link, position: style.position,
          transform: style.transform, visibility: style.visibility, display: style.display,
          box: { top: rect.top, bottom: rect.bottom, left: rect.left, right: rect.right } } };
    });
    report.samples.push({ stage, ...value }); await save();
    assert.equal(value.overflow, false, 'bounded keyboard event trace overflow');
    return value;
  }
  async function key(trial, index, key) {
    await sample(`${trial}-${index}-before-${key}`);
    await page.keyboard.press(key);
    await sample(`${trial}-${index}-after-${key}`);
    const frame = await page.evaluate(() => new Promise(resolve => {
      const timer = setTimeout(() => resolve(false), 1000);
      requestAnimationFrame(() => { clearTimeout(timer); resolve(true); });
    }));
    assert.equal(frame, true, 'next native animation frame unavailable');
    return sample(`${trial}-${index}-frame-${key}`);
  }
  try {
    report.delayed = [];
    const trials = ['pointer-without-full-page', 'post-lookup', 'keyboard-without-full-page',
      'known-input-without-full-page', 'known-input-with-full-page'];
    for (const name of trials) {
      {
        // A real same-document back updates Kit's router as well as the URL.
        // Native replaceState alone would leave Kit's current.url at #session,
        // making the following Enter look like a false keyboard failure.
        const previous = await sample(name + '-before-history-reset');
        if (previous.hash === '#session') {
          assert.equal(await page.goBack({ timeout: 2000 }), null);
          assert.equal(await page.evaluate(() => Boolean(window.__bigbrotrSkipTrace)), true, 'history reset replaced the document');
          await page.waitForFunction(() => window.__bigbrotrSkipTrace.events.some(event => event.type === 'popstate'), undefined, { timeout: 2000 });
        } else assert.equal(previous.hash, '');
        assert.equal(page.url(), entryUrl);
        await sample(name + '-after-history-reset');
      }
      if (name.startsWith('known-input') || name === 'keyboard-without-full-page') {
        await page.getByLabel('Job ID', { exact: true }).focus();
        await page.getByLabel('Job ID', { exact: true }).evaluate(input => input.scrollIntoView({ block: 'center', inline: 'nearest' }));
        const prepared = await sample(name + '-input-prepared');
        assert.equal(prepared.active.label, 'job-id'); assert.equal(prepared.documentFocused, true);
      }
      if (name === 'keyboard-without-full-page') {
        const ready = await key(name, 'prepare', 'Tab');
        assert.equal(ready.active.label, 'check-job'); assert.equal(ready.active.disabled, false);
        await lookup(() => page.keyboard.press('Enter'));
      } else if (name === 'pointer-padding') {
        const button = page.getByRole('button', { name: 'Check job', exact: true });
        await button.scrollIntoViewIfNeeded({ timeout: 2000 });
        const point = await button.evaluate(button => {
          const box = button.getBoundingClientRect();
          const walker = document.createTreeWalker(button, NodeFilter.SHOW_TEXT), textBoxes = [];
          while (walker.nextNode()) {
            const range = document.createRange(); range.selectNodeContents(walker.currentNode);
            for (const rect of range.getClientRects()) textBoxes.push({ left: rect.left, right: rect.right, top: rect.top, bottom: rect.bottom });
          }
          const x = box.left + 8, y = (box.top + box.bottom) / 2;
          return { x, y, box: { left: box.left, right: box.right, top: box.top, bottom: box.bottom }, textBoxes,
            inside: box.left < x && x < box.right && box.top < y && y < box.bottom,
            visible: x >= 0 && x < innerWidth && y >= 0 && y < innerHeight,
            outsideText: textBoxes.length > 0 && textBoxes.every(r => x < r.left || x > r.right || y < r.top || y > r.bottom),
            buttonAtPoint: document.elementFromPoint(x, y) === button };
        });
        report.padding = point; await save();
        assert.ok(point.inside && point.visible && point.outsideText && point.buttonAtPoint, 'verified padding click point');
        await sample(name + '-before-lookup');
        await lookup(() => page.mouse.click(point.x, point.y));
      } else if (!name.startsWith('known-input')) {
        await sample(name + '-before-lookup');
        await lookup(() => page.getByRole('button', { name: 'Check job', exact: true }).click({ timeout: 2000 }));
      }
      const lookedUp = await sample(name + '-after-lookup');
      if (name === 'post-lookup') {
        await page.screenshot({ path: directory + '/skip-before-viewport.png' });
        await page.screenshot({ path: directory + '/skip-before-full.png', fullPage: true });
        const captured = await sample('after-original-full-page');
        report.unfocusedAboveViewport = [lookedUp, captured].every(value =>
          !value.skip.focused && value.skip.position === 'fixed' && value.skip.box.bottom <= 0);
      }
      const start = await sample(name + '-start');
      if (name.startsWith('known-input')) {
        assert.equal(start.active.label, 'job-id'); assert.equal(start.documentFocused, true);
        assert.ok(start.inputBox && Math.abs((start.inputBox.top + start.inputBox.bottom - start.viewport.height) / 2) <= 1, 'controlled input must be centered within one CSS pixel');
      }
      if (name === 'known-input-with-full-page') {
        await page.screenshot({ path: directory + '/skip-known-input-full.png', fullPage: true });
        await sample(name + '-after-full-page');
      }
      let current = await sample(name + '-before-keys'), count = 0;
      if (current.skip.focused) {
        report.trials.push({ name, status: 'FAIL', reason: 'already-focused-before-keys' });
        await save(); continue;
      }
      while (!current.skip.focused && count < 8) current = await key(name, ++count, 'Shift+Tab');
      const result = { name, reverseTabs: count, status: 'UNREACHED', focused: current.skip.focused };
      report.trials.push(result); await save();
      if (!current.skip.focused && !name.startsWith('known-input')) {
        // A delayed extra key is a separate diagnostic, never a ninth attempt
        // that changes the original eight-key result. Do not refocus first.
        const before = await sample(name + '-before-passive-delay');
        await sleep(500);
        const after = await sample(name + '-after-passive-delay');
        const delayed = await key(name, 'delayed', 'Shift+Tab');
        report.delayed.push({ name, elapsedMs: after.monotonicMs - before.monotonicMs,
          before: before.active, afterWait: after.active, afterKey: delayed.active,
          focused: delayed.skip.focused }); await save();
      }
      if (!current.skip.focused) continue;
      const { box } = current.skip;
      result.visible = current.skip.visibility === 'visible' && current.skip.display !== 'none'
        && box.top >= 0 && box.left >= 0 && box.bottom <= current.viewport.height && box.right <= current.viewport.width;
      await page.screenshot({ path: `${directory}/skip-${name}-focused.png` });
      const entered = await key(name, 'activate', 'Enter');
      const next = await key(name, 'target', 'Tab');
      result.hashBefore = current.hash; result.hashAfter = entered.hash;
      result.next = next.active.label;
      result.status = result.visible && current.hash === '' && entered.hash === '#session'
        && next.active.label === 'sign-out' ? 'PASS' : 'FAIL';
      await save();
      if (name === 'keyboard-without-full-page' && result.status === 'PASS'
        && report.trials.find(trial => trial.name === 'pointer-without-full-page')?.status === 'UNREACHED') {
        // Follow up only an observed activation-path difference. The actual
        // browser hit-test starting node remains unobserved by this DOM probe.
        trials.splice(3, 0, 'pointer-padding');
      }
    }
    report.status = report.unfocusedAboveViewport && report.trials.every(value => value.status === 'PASS') ? 'PASS' : 'FAIL';
    return report;
  } catch (error) {
    report.status = 'INCOMPLETE'; report.failure = { kind: error?.constructor?.name ?? 'UnknownError' };
    throw error;
  } finally {
    try {
      await sample('terminal');
      await page.evaluate(() => { window.__bigbrotrSkipTrace.dispose(); delete window.__bigbrotrSkipTrace; });
    } catch (error) {
      report.status = 'INCOMPLETE'; report.cleanupError = { kind: error?.constructor?.name ?? 'UnknownError' };
      throw error;
    } finally { await save(); }
  }
}
