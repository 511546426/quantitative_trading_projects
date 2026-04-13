/** Last successful HTTP trace from axios (for status bar / support). */

let lastRequestId: string | null = null;
let lastServerTimeHeader: string | null = null;
const listeners = new Set<() => void>();

function notify() {
  listeners.forEach((fn) => fn());
}

export function setHttpTrace(requestId: string | null, serverTime: string | null) {
  lastRequestId = requestId;
  lastServerTimeHeader = serverTime;
  notify();
}

export function getHttpTrace() {
  return { requestId: lastRequestId, serverTimeHeader: lastServerTimeHeader };
}

export function subscribeHttpTrace(listener: () => void) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}
