import type { SessionView } from './session';

export const messages = {
  appTitle: 'BigBrotr · Private administration',
  privateAdministration: 'Private administration',
  instance: 'This instance',
  signInTitle: 'Sign in to this instance',
  signInHelp: 'Use the local account created during setup.',
  username: 'Username',
  password: 'Password',
  usernameHelp: 'Lowercase letters, numbers, dots, underscores and hyphens.',
  signIn: 'Sign in',
  signingIn: 'Signing in…',
  checking: 'Checking your session…',
  signingOut: 'Signing out…',
  signOut: 'Sign out',
  confirmSignOut: 'Confirm sign-out',
  sessionTitle: 'Your session',
  sessionHelp: 'You are signed in to the instance shown here.',
  principal: 'Account ID',
  idleExpiry: 'Inactive session ends',
  absoluteExpiry: 'Session ends by',
  actions: 'Account permissions',
  actionsHelp: 'Each operation checks your current access. These permissions do not grant private export or signing access.',
  noActions: 'No permissions are stored for this account.',
  refresh: 'Refresh session',
  refreshHelp: 'Refresh checks your account and counts as activity. This page does not keep your session alive in the background.',
  initialScope: 'Initial private panel · Session and export status',
  scriptRequired: 'JavaScript is required to sign in. Your local administration commands remain available.',
  checkingFailed: 'The session could not be checked. Check that this instance is running, then try again.',
  incompatible: 'This panel and the instance returned incompatible session information. Reload the panel or check that their versions match.',
  invalidInput: 'Check your username and password. Passwords need at least 12 characters and cannot contain control characters.',
  denied: 'Sign-in was refused. Check your account details and try again.',
  refused: 'This browser request was refused. Open the configured private address and try again.',
  throttled: 'The attempt or capacity limit was reached. Wait at least one minute before trying again.',
  loginUnknown: 'Sign-in could not be confirmed. It may have completed. Check your session or make a new sign-in attempt.',
  logoutUnknown: 'Sign-out could not be confirmed. Your account details have been cleared from this page. Confirm sign-out again before leaving a shared browser.',
  csrfMissing: 'The sign-out token is missing or ambiguous. Open the configured private address and check your session before trying again.',
  signedOut: 'You have signed out.',
  expired: 'This session observation has expired. Sign in again to continue.',
  changed: 'Your session changed in another tab. Check your session or sign in again.',
  recheck: 'Check your session after returning to this page.',
  checkSession: 'Check session',
  requestId: 'Request ID',
  skip: 'Skip to session',
  utc: 'UTC'
} as const;

export function notice(view: SessionView): string | undefined {
  const failure = view.failure;
  if (!failure) return view.notice === 'signed-out' ? messages.signedOut
    : view.notice === 'changed' ? messages.changed : view.notice === 'expired' ? messages.expired : view.notice === 'recheck' ? messages.recheck : undefined;
  if (failure.kind === 'incompatible' || failure.kind === 'response_limit') return messages.incompatible;
  if (failure.kind === 'invalid_input') return messages.invalidInput;
  if (failure.kind === 'csrf_missing') return messages.csrfMissing;
  if (view.failedOperation === 'logout') return messages.logoutUnknown;
  if (failure.api?.status === 429) return messages.throttled;
  if (failure.api?.status === 401) return messages.denied;
  if (failure.api?.status === 403 || failure.api?.status === 400) return messages.refused;
  return view.failedOperation === 'login' ? messages.loginUnknown : messages.checkingFailed;
}

export function utcTime(microseconds: string): string {
  const milliseconds = BigInt(microseconds) / 1000n;
  if (milliseconds > 8_640_000_000_000_000n || milliseconds < -8_640_000_000_000_000n) return 'Date outside display range';
  return new Intl.DateTimeFormat('en', { dateStyle: 'medium', timeStyle: 'medium', timeZone: 'UTC' }).format(Number(milliseconds)) + ' UTC';
}
