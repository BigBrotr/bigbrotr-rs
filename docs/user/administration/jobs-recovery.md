# Inspecting and resuming your export

The current private panel can inspect an existing export and resume a paused
export owned by your account. Job creation, cancellation, listing and downloads
are still being implemented. The [export operations guide](../../operations/export.md)
describes the current API and deployment prerequisites.

Sign in at your instance's private address. In **Inspect your job**, paste the
32-character job ID from your acceptance receipt and select **Check job**. The
panel shows the stored state and when it was observed. Check again for changes;
this view does not refresh automatically or keep your session alive.

If the job is paused, resolve its resource or permission problem with the instance
owner, then select **Resume this export**. Resume keeps the original selection,
destination and limits. Your current access must allow the original export; being
an administrator or being able to inspect a job does not grant access to its data.
See [scoped permissions](scope-grants.md).

Before sending, the panel saves a recovery request for your account in this
browser. **Resume accepted** means the command was accepted. Select **Check saved
job** to observe whether execution has started or finished. The saved outcome
alone does not show progress or confirm that a file is available.

If the reply is lost or cannot be confirmed, keep the saved request. Reload the
same private address in the same browser profile and sign in with the same
account if needed. The **Saved resume request** card returns without sending
anything. Select **Retry saved request** to resolve its original outcome. Retry
keeps the original request key and revisions; it does not resume a later pause.
A new attempt after a later pause requires a fresh job observation.

Signing out clears private views in open tabs while keeping recovery metadata.
Another account does not see your saved request. Clearing this site's browser
data, switching profiles or losing browser storage can remove that local metadata;
it does not cancel the server's job. Keep your original job receipt as well.

If the session ends, sign in again. If capacity is exhausted, wait as instructed
before explicitly retrying. A permissions or dependency failure keeps the
request available for recovery. A conflict is a refusal: refresh the session and
check the job before starting another resume. If browser storage cannot be read
or saved, preserve its data and ask the instance owner for help; do not assume
that an earlier request failed on the server.

**Forget resolved request** removes only an accepted or refused recovery record
from this browser. It changes no job or server receipt. Unconfirmed requests
cannot be forgotten through this control. The browser keeps one request per
account, so resolve that request before submitting a different one.

[Scoped native evidence](../../implementation/evidence/p09-resume-browser-native-20260924.json)
covers this installed Chromium journey on Linux amd64, including reload after a
withheld real acceptance, server restart and account revocation. Native storage
checks also ran on Firefox and WebKit. The complete browser/deployment matrix,
accessibility and release installation remain open qualification work.
