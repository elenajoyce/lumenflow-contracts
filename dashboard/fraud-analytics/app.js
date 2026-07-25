/* Fraud Analytics Dashboard — app.js
 *
 * Issue #651: Migrate from polling to SSE streaming
 *
 * Features:
 *  - EventSource opened on page load, closed on page unload/hide
 *  - Streams suspicious_activity events from Horizon in real time
 *  - Reconnect strategy: re-opens SSE after 10 s on disconnect
 *  - Live (green) / Reconnecting (yellow) status indicator in header
 *  - Polling fallback when SSE is unavailable (e.g. network proxy blocks it)
 */

// ── Configuration ────────────────────────────────────────────────────────────

const HORIZON_BASE           = window.HORIZON_BASE || 'https://horizon-testnet.stellar.org';
const CONTRACT_ID            = window.CONTRACT_ID  || null;
const SSE_RECONNECT_DELAY_MS = 10_000;   // reconnect after 10 s per spec
const POLL_INTERVAL_MS       = 15_000;
const MAX_EVENTS             = 200;

// ── State ────────────────────────────────────────────────────────────────────

const state = {
  events:      [],
  metrics: {
    total:     0,
    high:      0,
    medium:    0,
    addresses: new Set(),
  },
  sse:         null,   // active EventSource
  pollTimer:   null,   // fallback poll interval handle
  sseFailed:   false,  // true once first SSE error has fired
};

// ── DOM refs ─────────────────────────────────────────────────────────────────

const streamStatusEl = document.getElementById('streamStatus');
const eventsBodyEl   = document.getElementById('eventsBody');
const eventsTableEl  = document.getElementById('eventsTable');
const emptyStateEl   = document.getElementById('emptyState');
const metricTotalEl  = document.getElementById('metricTotal');
const metricHighEl   = document.getElementById('metricHigh');
const metricMedEl    = document.getElementById('metricMedium');
const metricAddrsEl  = document.getElementById('metricAddresses');

// ── Status indicator ─────────────────────────────────────────────────────────

/**
 * Updates the header indicator.
 * @param {'live'|'reconnecting'|'polling'} status
 */
function setStreamStatus(status) {
  if (!streamStatusEl) return;
  streamStatusEl.className = 'status-indicator';
  if (status === 'live') {
    streamStatusEl.classList.add('live');
    streamStatusEl.textContent = 'Live';
  } else if (status === 'polling') {
    streamStatusEl.classList.add('reconnecting');
    streamStatusEl.textContent = 'Polling (fallback)';
  } else {
    streamStatusEl.classList.add('reconnecting');
    streamStatusEl.textContent = 'Reconnecting…';
  }
}

// ── Rendering ─────────────────────────────────────────────────────────────────

function severityFromReason(reason) {
  const r = String(reason || '').toLowerCase();
  if (r.includes('large') || r.includes('auth'))   return 'high';
  if (r.includes('rapid') || r.includes('refund')) return 'medium';
  return 'low';
}

function formatAddress(addr) {
  if (!addr || addr.length < 12) return addr || '—';
  return addr.slice(0, 6) + '…' + addr.slice(-4);
}

function formatTime(iso) {
  try { return new Date(iso).toLocaleTimeString(); } catch { return iso || '—'; }
}

function formatAmount(amount) {
  const n = Number(amount);
  if (amount == null || isNaN(n)) return '—';
  return (n / 1e7).toFixed(7) + ' XLM';
}

function renderTable() {
  if (!state.events.length) {
    emptyStateEl && (emptyStateEl.style.display = 'block');
    eventsTableEl && (eventsTableEl.style.display = 'none');
    return;
  }
  emptyStateEl && (emptyStateEl.style.display = 'none');
  eventsTableEl && (eventsTableEl.style.display = 'table');

  if (!eventsBodyEl) return;
  eventsBodyEl.innerHTML = '';
  state.events.forEach(evt => {
    const sev = severityFromReason(evt.reason);
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${formatTime(evt.time)}</td>
      <td title="${evt.address || ''}">${formatAddress(evt.address)}</td>
      <td>${evt.reason || '—'}</td>
      <td>${formatAmount(evt.amount)}</td>
      <td><span class="badge badge-${sev}">${sev}</span></td>
    `;
    eventsBodyEl.appendChild(tr);
  });
}

function updateMetrics() {
  metricTotalEl  && (metricTotalEl.textContent  = state.metrics.total);
  metricHighEl   && (metricHighEl.textContent   = state.metrics.high);
  metricMedEl    && (metricMedEl.textContent    = state.metrics.medium);
  metricAddrsEl  && (metricAddrsEl.textContent  = state.metrics.addresses.size);
}

function addEvent(evt) {
  state.events.unshift(evt);
  if (state.events.length > MAX_EVENTS) state.events.pop();

  state.metrics.total++;
  const sev = severityFromReason(evt.reason);
  if (sev === 'high')   state.metrics.high++;
  if (sev === 'medium') state.metrics.medium++;
  if (evt.address) state.metrics.addresses.add(evt.address);

  updateMetrics();
  renderTable();
}

// ── Event parsing ─────────────────────────────────────────────────────────────

function parseHorizonEvent(raw) {
  try {
    const data = typeof raw === 'string' ? JSON.parse(raw) : raw;
    return {
      time:    data.created_at || new Date().toISOString(),
      address: data.payer || data.address || (Array.isArray(data.value) ? data.value[0] : null) || null,
      reason:  (data.topic && data.topic[1]) || data.reason || 'suspicious_activity',
      amount:  data.amount ?? null,
    };
  } catch {
    return { time: new Date().toISOString(), reason: 'suspicious_activity' };
  }
}

// ── SSE streaming ─────────────────────────────────────────────────────────────

let _reconnectTimer = null;

/**
 * Opens a Horizon SSE connection for suspicious_activity contract events.
 * On error, schedules a reconnect after SSE_RECONNECT_DELAY_MS.
 * After the second consecutive failure, falls back to polling.
 */
function openSse() {
  if (state.sse) { state.sse.close(); state.sse = null; }
  clearTimeout(_reconnectTimer);

  const url = CONTRACT_ID
    ? `${HORIZON_BASE}/events?contract_id=${CONTRACT_ID}&topic=lumenflow%2Fsuspicious_activity&cursor=now`
    : `${HORIZON_BASE}/events?cursor=now`;

  let es;
  try {
    es = new EventSource(url);
  } catch (err) {
    console.warn('[fraud-analytics] EventSource() threw:', err);
    fallbackToPolling();
    return;
  }

  state.sse = es;
  setStreamStatus('reconnecting');

  es.addEventListener('open', () => {
    setStreamStatus('live');
    state.sseFailed = false;
  });

  // Horizon named event for contract events
  es.addEventListener('events', (e) => {
    try {
      const parsed = parseHorizonEvent(e.data);
      if (!parsed.reason || parsed.reason.includes('suspicious')) addEvent(parsed);
    } catch (err) { console.warn('[fraud-analytics] Event parse error:', err); }
  });

  // Generic fallback for plain SSE messages
  es.addEventListener('message', (e) => {
    try { addEvent(parseHorizonEvent(e.data)); }
    catch (err) { console.warn('[fraud-analytics] Message parse error:', err); }
  });

  es.addEventListener('error', () => {
    es.close();
    state.sse = null;
    setStreamStatus('reconnecting');

    if (!state.sseFailed) {
      // First error: schedule a reconnect after 10 s
      _reconnectTimer = setTimeout(openSse, SSE_RECONNECT_DELAY_MS);
    } else {
      // Second consecutive error: give up on SSE, use polling fallback
      fallbackToPolling();
    }
    state.sseFailed = true;
  });
}

// ── Polling fallback ─────────────────────────────────────────────────────────

async function pollOnce() {
  if (!CONTRACT_ID) {
    // Demo mode: inject a fake event so the dashboard isn't empty
    injectDemoEvent();
    return;
  }
  try {
    const url = `${HORIZON_BASE}/events?contract_id=${CONTRACT_ID}&topic=lumenflow%2Fsuspicious_activity&order=desc&limit=20`;
    const resp = await fetch(url, { headers: { Accept: 'application/json' } });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const json = await resp.json();
    const records = json._embedded?.records || json.records || [];
    records.forEach(r => addEvent(parseHorizonEvent(r)));
  } catch (err) {
    console.warn('[fraud-analytics] Poll error:', err);
  }
}

function fallbackToPolling() {
  if (state.pollTimer) return; // already polling
  setStreamStatus('polling');
  pollOnce();
  state.pollTimer = setInterval(pollOnce, POLL_INTERVAL_MS);
}

// ── Demo mode ─────────────────────────────────────────────────────────────────

const _DEMO_REASONS = ['LargePayment', 'RapidRefunds', 'ManyAuthFailures'];
let _demoCounter = 0;

function injectDemoEvent() {
  const reason = _DEMO_REASONS[_demoCounter % _DEMO_REASONS.length];
  _demoCounter++;
  addEvent({
    time:    new Date().toISOString(),
    address: `GDEMO${String(_demoCounter).padStart(5, '0')}AAAAAAAAAAAAAAAAAAAAAAAAAAAA`,
    reason,
    amount:  Math.floor(Math.random() * 1_000_000_000),
  });
}

// ── Dark mode ─────────────────────────────────────────────────────────────────

function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  localStorage.setItem('lumenflow_theme', theme);
  const btn = document.getElementById('themeToggle');
  if (btn) btn.textContent = theme === 'dark' ? '☀️ Light' : '🌙 Dark';
}

const themeBtn = document.getElementById('themeToggle');
if (themeBtn) {
  // Sync label with the theme applied by the inline script
  const initial = document.documentElement.getAttribute('data-theme') || 'light';
  themeBtn.textContent = initial === 'dark' ? '☀️ Light' : '🌙 Dark';
  themeBtn.addEventListener('click', () => {
    const cur = document.documentElement.getAttribute('data-theme') || 'light';
    applyTheme(cur === 'dark' ? 'light' : 'dark');
  });
}

// ── Lifecycle ─────────────────────────────────────────────────────────────────

function init() {
  if (typeof EventSource === 'undefined') {
    // Environment does not support SSE — go straight to polling
    console.info('[fraud-analytics] EventSource not supported; using polling fallback.');
    fallbackToPolling();
  } else {
    openSse();
  }

  // Demo mode: pre-populate with a few synthetic events
  if (!CONTRACT_ID) {
    for (let i = 0; i < 5; i++) injectDemoEvent();
  }
}

// Close the SSE connection when the page is hidden/unloaded
window.addEventListener('pagehide', () => {
  if (state.sse) { state.sse.close(); state.sse = null; }
  clearInterval(state.pollTimer);
  clearTimeout(_reconnectTimer);
});

init();
