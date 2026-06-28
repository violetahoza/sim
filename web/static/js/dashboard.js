Chart.defaults.color = '#8b91a8';
Chart.defaults.borderColor = '#2a2f45';
Chart.defaults.font.family = "'DM Sans', sans-serif";

const C = { cyan: '#00e5c8', blue: '#4d9bff', amber: '#ffc246', red: '#ff5252', green: '#39e887', purple: '#b47cff', dim: '#555a72'};

let charts = {};
let allResults = [];
let currentResult = null;
let simRunning = false;
let currentSpotCount = 0;
let allScenarios = [];
let currentArchitecture = null;

document.addEventListener('DOMContentLoaded', async () => {
  initCharts();
  await loadScenarios();
  connectSSE();
  await loadSavedResults();
});

function pick(raw, ...names) {
  if (!raw) return null;
  for (const n of names) {
    const v = raw[n];
    if (v !== undefined && v !== null) return v;
  }
  return null;
}

function normalizeMetrics(raw) {
  if (!raw) return {};
  const m = {
    scenario_name: raw.scenario_name, seed: raw.seed, run_id: raw.run_id,
    protocol: raw.protocol, architecture: raw.architecture,
    traffic_level: raw.traffic_level, num_spots: raw.num_spots,
    sim_duration_s: raw.sim_duration_s, heartbeat_interval_s: raw.heartbeat_interval_s,
    source: raw.source, scenario_log: raw.scenario_log, latency_samples: raw.latency_samples,
    fault_injected_count: pick(raw, 'fault_injected_count'),
    final_spot_states: raw.final_spot_states, final_occupancy: raw.final_occupancy,

    events_generated_total: pick(raw, 'events_generated_total', 'events_generated'),
    state_changes_generated_total: pick(raw, 'state_changes_generated_total', 'valid_state_changes'),
    heartbeats_generated_total: pick(raw, 'heartbeats_generated_total', 'heartbeats_generated'),
    initial_snapshots_generated_total: pick(raw, 'initial_snapshots_generated_total', 'initial_snapshots_generated'),
    duplicate_sends_generated_total: pick(raw, 'duplicate_sends_generated_total', 'duplicate_sends_generated'),

    frames_s2e_sent: pick(raw, 'frames_s2e_sent', 'sensor_to_edge_msgs'),
    frames_s2e_delivered: pick(raw, 'frames_s2e_delivered', 'wire_frames_delivered'),
    frames_s2e_dropped: pick(raw, 'frames_s2e_dropped', 'sensor_link_dropped'),
    bytes_s2e_sent: pick(raw, 'bytes_s2e_sent', 'sensor_to_edge_bytes'),
    bytes_s2e_received: pick(raw, 'bytes_s2e_received'),
    s2e_delivery_ratio: pick(raw, 's2e_delivery_ratio', 'sensor_to_edge_delivery_ratio'),

    frames_e2c_sent: pick(raw, 'frames_e2c_sent', 'edge_to_cloud_msgs'),
    frames_e2c_delivered: pick(raw, 'frames_e2c_delivered'),
    frames_e2c_dropped: pick(raw, 'frames_e2c_dropped', 'edge_to_cloud_dropped'),
    bytes_e2c_sent: pick(raw, 'bytes_e2c_sent', 'edge_to_cloud_bytes'),
    bytes_e2c_received: pick(raw, 'bytes_e2c_received'),
    proto_bytes_sent: pick(raw, 'proto_bytes_sent', 'protocol_bytes'),
    proto_retransmissions: pick(raw, 'proto_retransmissions', 'retransmissions_total'),
    proto_duplicate_deliveries: pick(raw, 'proto_duplicate_deliveries', 'duplicate_deliveries'),
    backhaul_delivery_ratio: pick(raw, 'backhaul_delivery_ratio'),

    unique_state_changes_applied_at_cloud: pick(raw, 'unique_state_changes_applied_at_cloud', 'cloud_state_changes_reflected'),
    duplicate_events_at_cloud: pick(raw, 'duplicate_events_at_cloud'),
    e2e_unique_delivery_ratio: pick(raw, 'e2e_unique_delivery_ratio'),
    cloud_reflection_ratio: pick(raw, 'cloud_reflection_ratio', 'cloud_state_agreement_ratio'),
    cloud_msgs_received: pick(raw, 'cloud_events_pre_dedup', 'cloud_msgs_received', 'cloud_msgs_received_total'),
    cloud_batches_received: pick(raw, 'cloud_batches_received'),
    cloud_events_post_dedup: pick(raw, 'cloud_events_post_dedup'),
    physical_delivery_ratio: pick(raw, 'physical_delivery_ratio'),

    latency_mean_ms: pick(raw, 'latency_mean_ms'),
    latency_p50_ms: pick(raw, 'latency_p50_ms'),
    latency_p95_ms: pick(raw, 'latency_p95_ms'),
    latency_p99_ms: pick(raw, 'latency_p99_ms'),
    latency_min_ms: pick(raw, 'latency_min_ms'),
    latency_max_ms: pick(raw, 'latency_max_ms'),

    events_filtered_total: pick(raw, 'events_filtered_total', 'filtered_events'),
    events_forwarded_total: pick(raw, 'events_forwarded_total'),
    aggregation_ratio: pick(raw, 'aggregation_ratio'),
    message_reduction_ratio: pick(raw, 'message_reduction_ratio'),
    heartbeats_suppressed: pick(raw, 'heartbeats_suppressed'),
    heartbeats_forwarded: pick(raw, 'heartbeats_forwarded'),
    quarantine_suppressed: pick(raw, 'quarantine_suppressed'),
    anomalies_detected: pick(raw, 'anomalies_detected'),
    anomalies_resolved: pick(raw, 'anomalies_resolved'),
    active_anomalies: pick(raw, 'active_anomalies'),
    adaptive_mode_switches: pick(raw, 'adaptive_mode_switches'),
    quarantined_spots_final: pick(raw, 'quarantined_spots_final'),
    anomaly_detected_spots: pick(raw, 'anomaly_detected_spots'),
  };
  m.events_per_cloud_message = (m.events_forwarded_total != null && m.frames_e2c_sent) ? m.events_forwarded_total / m.frames_e2c_sent : null;
  return m;
}


const isCloudOnly = r => r?.architecture === 'cloud_only';
const isEdgeAgg = r => r?.architecture === 'edge_aggregated';
const isEdge = r => !isCloudOnly(r);
const hasAnomaly = r => isEdge(r) && ((r.anomalies_detected ?? 0) > 0 || (r.adaptive_mode_switches ?? 0) > 0 || (r.quarantined_spots_final ?? 0) > 0);

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val ?? '—';
}

async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`fetchJSON ${url} → ${r.status}`);
  return r.json();
}

async function loadScenarios() {
  allScenarios = await fetchJSON('/api/scenarios');
  const sel = document.getElementById('presetSelect');
  sel.innerHTML = '';

  const groups = {}, ungrouped = [];
  allScenarios.forEach(s => {
    if (s.group) (groups[s.group] = groups[s.group] || []).push(s);
    else ungrouped.push(s);
  });
  const sortedGroupKeys = Object.keys(groups).sort((a, b) => {
    const minA = Math.min(...groups[a].map(s => s.group_order));
    const minB = Math.min(...groups[b].map(s => s.group_order));
    return minA - minB;
  });
  sortedGroupKeys.forEach(gk => groups[gk].sort((a, b) => a.group_order - b.group_order));

  const mkOpt = s => {
    const o = document.createElement('option');
    o.value = s.name;
    const lbl = s.description.split('|')[0].trim();
    o.textContent = lbl.length > 56 ? lbl.slice(0, 53) + '…' : lbl;
    return o;
  };

  sortedGroupKeys.forEach(gk => {
    const og = document.createElement('optgroup');
    og.label = gk;
    groups[gk].forEach(s => og.appendChild(mkOpt(s)));
    sel.appendChild(og);
  });
  ungrouped.forEach(s => sel.appendChild(mkOpt(s)));

  sel.addEventListener('change', () => {
    const name = sel.value;
    const full = allScenarios.find(s => s.name === name);
    const rawDesc = full?.description || '';
    setText('presetDesc', rawDesc.includes('|') ? rawDesc.split('|').slice(1).join('|').trim() : rawDesc);
    setText('presetMeta', full
      ? `${(full.protocol || '').toUpperCase()} · ${full.architecture} · ${full.traffic_level} · ${full.num_spots} spots · ${(full.sim_duration_s / 3600).toFixed(1)} h` : '');
    const hint = document.getElementById('groupHint');
    if (hint && full?.group) {
      const sibs = allScenarios.filter(s => s.group === full.group);
      hint.textContent = sibs.length > 1 ? `💡 Part of "${full.group}" — run all ${sibs.length} to compare` : '';
      hint.style.display = sibs.length > 1 ? 'block' : 'none';
    } else if (hint) hint.style.display = 'none';
  });
  sel.dispatchEvent(new Event('change'));
}

async function loadSavedResults() {
  const results = await fetchJSON('/api/results');
  results.forEach(r => addResultRow(r, false));
  if (results.length) {
    allResults = results;
    showResult(results[results.length - 1]);
    updateComparisonChart();
    updateDeliveryChart();
    updateAIBadge();
  }
}

function switchTab(ev, name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t => t.classList.add('hidden'));
  ev.currentTarget.classList.add('active');
  document.getElementById(`tab-${name}`).classList.remove('hidden');
}

function onProtocolChange() {
  const p = document.getElementById('c_protocol').value;
  document.querySelectorAll('.proto-opts').forEach(el => el.classList.add('hidden'));
  document.getElementById(`${p}-opts`)?.classList.remove('hidden');
}

async function runPreset() {
  if (simRunning) return;
  const name = document.getElementById('presetSelect').value;
  if (!name) return;
  await startRun('/api/run/preset/' + name, null);
}

async function runCustom() {
  if (simRunning) return;
  const get = id => document.getElementById(id)?.value ?? '';
  const body = {
    protocol: get('c_protocol'),
    architecture: get('c_arch'),
    traffic_level: get('c_traffic'),
    num_spots: +get('c_spots'),
    loss_rate: +get('c_loss') / 100,
    sim_duration_h: +get('c_duration'),
    rate_limit: +get('c_ratelimit'),
    aggregation_interval: +get('c_agg'),
    amqp_exchange: get('c_amqp_exchange') || 'direct',
    amqp_ack: get('c_amqp_ack') || 'manual',
    amqp_durable: (get('c_amqp_durable') || 'true') === 'true',
    mqtt_qos: +(get('c_qos') || '1'),
    coap_mode: get('c_coap_mode') || 'CON',
  };
  await startRun('/api/run/custom', body);
}

async function startRun(url, body) {
  const resp = await fetch(url, {
    method: 'POST',
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({}));
    alert('Failed to start: ' + (err.detail || resp.status));
  }
}

async function stopSim() {
  if (!simRunning) return;
  await fetch('/api/stop', { method: 'POST' }).catch(() => {});
}

async function clearResults() {
  if (!confirm('Clear all run history?')) return;
  await fetch('/api/results', { method: 'DELETE' }).catch(() => {});
  allResults = [];
  const c = document.getElementById('resultsTable');
  if (c) c.innerHTML = '<div class="results-empty">No runs yet. Start a scenario.</div>';
  updateAIBadge();
}

function setRunning(running) {
  simRunning = running;
  const sb = document.getElementById('stopBtn');
  const pb = document.getElementById('progressBarWrap');
  if (sb) sb.style.display = running ? 'block' : 'none';
  if (pb) pb.style.display = running ? 'block' : 'none';
  const dot = document.getElementById('statusDot');
  const txt = document.getElementById('statusText');
  if (dot) dot.className = 'status-indicator ' + (running ? 'running' : 'idle');
  if (txt) txt.textContent = running ? 'RUNNING' : 'IDLE';
  if (!running) {
    const pf = document.getElementById('progressBarFill');
    const pl = document.getElementById('progressBarLabel');
    if (pf) pf.style.width = '0%';
    if (pl) pl.textContent = '';
  }
}

function connectSSE() {
  const es = new EventSource('/api/stream');
  es.addEventListener('sim_started', e => {
    const d = JSON.parse(e.data);
    setRunning(true);
    buildLotGrid(d.num_spots || 50, true);
    const ls = document.getElementById('lotStats');
    if (ls) ls.style.display = 'grid';
    resetLiveSection();
    currentArchitecture = d.architecture || null;
    const cloudOnly = currentArchitecture === 'cloud_only';
    ['progFilteredItem', 'progAnomaliesItem'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.style.display = cloudOnly ? 'none' : '';
    });
  });
  es.addEventListener('progress', e => onProgress(JSON.parse(e.data)));
  es.addEventListener('sim_flushing', () => {
    const pf = document.getElementById('progressBarFill');
    const pl = document.getElementById('progressBarLabel');
    if (pf) pf.style.width = '100%';
    if (pl) pl.textContent = 'Saving...';
    const dot = document.getElementById('statusDot');
    const txt = document.getElementById('statusText');
    if (dot) dot.className = 'status-indicator running';
    if (txt) txt.textContent = 'SAVING…';
  });
  es.addEventListener('sim_complete', e => {
    const r = JSON.parse(e.data);
    setRunning(false);
    const dot = document.getElementById('statusDot');
    const txt = document.getElementById('statusText');
    if (dot) dot.className = 'status-indicator done';
    if (txt) txt.textContent = 'DONE';
    allResults.push(r);
    addResultRow(r, true);
    showResult(r);
    updateComparisonChart();
    updateDeliveryChart();
    updateAIBadge();
  });
  es.addEventListener('sim_error', e => {
    const d = JSON.parse(e.data);
    setRunning(false);
    const dot = document.getElementById('statusDot');
    const txt = document.getElementById('statusText');
    if (dot) dot.className = 'status-indicator error';
    if (txt) txt.textContent = 'ERROR: ' + (d.error || '?');
  });
  es.onerror = () => { es.close(); setTimeout(connectSSE, 3000); };
}

function onProgress(d) {
  const pct = d.progress_pct || 0;
  const pf = document.getElementById('progressBarFill');
  const pl = document.getElementById('progressBarLabel');
  if (pf) pf.style.width = pct + '%';
  if (pl) pl.textContent = pct + '%';
  setText('progElapsed', d.elapsed_s ?? '-');
  const simSec = d.simulated_elapsed_s ?? 0;
  const simH = Math.floor(simSec / 3600);
  const simM = Math.floor((simSec % 3600) / 60);
  setText('progSimTime', `${simH}h ${String(simM).padStart(2, '0')}m`);
  setText('progGenerated', d.generated ?? '-');
  setText('progHeartbeats', d.heartbeats ?? '-');
  setText('progReceived', d.cloud_events ?? '-');
  const occ  = d.occupancy || {};
  const edge = d.edge || {};
  setText('progOccupancy', occ.occupancy_pct != null ? occ.occupancy_pct + '%' : '-');
  setText('progFiltered',  edge.filtered  ?? '-');
  setText('progAnomalies', edge.anomalies ?? '-');
  const spots = d.spot_states;
  if (spots) {
    const n = Object.keys(spots).length;
    if (n && n !== currentSpotCount) buildLotGrid(n);
    Object.entries(spots).forEach(([id, st]) => updateSpot(+id, st));
  }
  if (occ.total != null) {
    setText('lst_total', occ.total);
    setText('lst_free', occ.free);
    setText('lst_occ', occ.occupied);
    setText('lst_pct', occ.occupancy_pct + '%');
  }
}

function resetLiveSection() {
  ['progElapsed', 'progGenerated', 'progHeartbeats', 'progReceived',
   'progOccupancy', 'progFiltered', 'progAnomalies'].forEach(id => setText(id, '-'));
}

function buildLotGrid(n, force) {
  if (n === currentSpotCount && !force) return;
  currentSpotCount = n;
  const grid = document.getElementById('lotGrid');
  if (!grid) return;
  grid.innerHTML = '';
  const visualN = Math.min(n, 500);
  for (let i = 0; i < visualN; i++) {
    const d = document.createElement('div');
    d.className = 'spot free';
    d.id = `spot_${i}`;
    d.title = `Spot ${i}`;
    grid.appendChild(d);
  }
  if (n > 500) {
    const note = document.createElement('div');
    note.style.cssText =
      'grid-column:1/-1;font-size:.65rem;color:var(--dim);text-align:center;padding:.25rem 0';
    note.textContent = `Showing first 500 of ${n} spots`;
    grid.appendChild(note);
  }
  const ls = document.getElementById('lotStats');
  if (ls) ls.style.display = 'grid';
  setText('lst_total', n);
  setText('lst_free', n);
  setText('lst_occ', 0);
  setText('lst_pct', '0%');
}

function updateSpot(id, state) {
  const el = document.getElementById(`spot_${id}`);
  if (!el) return;
  const s = String(state);
  if (el.dataset.state === s) return;
  el.dataset.state = s;
  el.className = `spot ${s} flash`;
}

function restoreLotFromResult(m) {
  const n = m.num_spots;
  if (!n) return;

  buildLotGrid(n, true);

  const spots = m.final_spot_states;
  const occ = m.final_occupancy;

  if (spots && Object.keys(spots).length) {
    Object.entries(spots).forEach(([id, st]) => updateSpot(+id, st));
  }

  if (occ && occ.total != null) {
    setText('lst_total', occ.total);
    setText('lst_free', occ.free);
    setText('lst_occ', occ.occupied);
    setText('lst_pct', occ.occupancy_pct + '%');
  }

  setText('progOccupancy', occ && occ.occupancy_pct != null ? occ.occupancy_pct + '%' : '-');
  setText('progGenerated', m.events_generated_total ?? '-');
  setText('progHeartbeats', m.heartbeats_generated_total ?? '-');
  setText('progReceived', m.cloud_msgs_received ?? '-');
  setText('progFiltered', m.events_filtered_total ?? '-');
  setText('progAnomalies', m.anomalies_detected ?? '-');
  const simH = Math.floor((m.sim_duration_s ?? 0) / 3600);
  const simM = Math.floor(((m.sim_duration_s ?? 0) % 3600) / 60);
  setText('progSimTime', `${simH}h ${String(simM).padStart(2, '0')}m`);
  setText('progElapsed', '-');

  const cloudOnly = m.architecture === 'cloud_only';
  ['progFilteredItem', 'progAnomaliesItem'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = cloudOnly ? 'none' : '';
  });
}

function addResultRow(r, scrollIntoView) {
  const container = document.getElementById('resultsTable');
  if (!container) return;
  container.querySelector('.results-empty')?.remove();
  const m = normalizeMetrics(r);
  const row = document.createElement('div');
  row.className = 'result-row';
  const protoClass = m.protocol === 'amqp' ? 'tag-amqp' : `tag-${m.protocol}`;
  const historicalBadge = r.source === 'historical' ? '<span class="result-tag tag-historical">saved</span>' : '';
  const durationLabel = m.sim_duration_s ? `${(m.sim_duration_s / 3600).toFixed(1)}h` : '';
  const logBadge = m.scenario_log?.length > 0 ? '<span class="result-tag" style="color:var(--purple)">log</span>' : '';
  row.innerHTML = `
    <div class="result-name" title="${m.scenario_name}">${m.scenario_name}</div>
    <div class="result-meta">
      ${historicalBadge}
      <span class="result-tag ${protoClass}">${(m.protocol || '').toUpperCase()}</span>
      <span class="result-tag">${m.architecture || ''}</span>
      <span class="result-tag">${m.traffic_level || ''}</span>
      ${durationLabel ? `<span class="result-tag">${durationLabel}</span>` : ''}
      ${logBadge}
      <span class="result-lat">${m.latency_mean_ms != null ? m.latency_mean_ms.toFixed(1) + 'ms' : 'N/A'}</span>
    </div>`;
  row.onclick = () => {
    document.querySelectorAll('.result-row').forEach(rr => rr.classList.remove('active'));
    row.classList.add('active');
    showResult(r);
  };
  container.insertBefore(row, container.firstChild);
  if (scrollIntoView) row.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  document.querySelectorAll('.result-row').forEach(rr => rr.classList.remove('active'));
  row.classList.add('active');
}

function updateAIBadge() {
  const badge = document.getElementById('aiBadge');
  if (!badge) return;
  badge.textContent = allResults.length;
  badge.style.display = allResults.length > 0 ? 'inline-flex' : 'none';
}

function showResult(r) {
  currentResult = r;
  const m = normalizeMetrics(r);
  renderKpiStrip(m);
  renderMetricsTable(m);
  updateLatencyHistogram(m.latency_samples || []);
  updateMsgCountChart(m);
  updateBandwidthChart(m);
  restoreLotFromResult(m);
}

function toggleMetricsTable() {
  const wrap = document.getElementById('metricsTableWrap');
  const lbl  = document.getElementById('metricsToggleLabel');
  const open = wrap.classList.toggle('open');
  if (lbl) lbl.textContent = open ? '▲ Full Metrics' : '▼ Full Metrics';
}

const NA = 'N/A';
const _fmtV = (v, unit = '', dp = 1) => v != null ? `${(+v).toFixed(dp)}${unit}` : NA;
const _fmtInt = v => v != null ? Number(v).toLocaleString() : NA;
const _fmtPct = (v, dp = 2) => v != null ? `${(v * 100).toFixed(dp)}%` : NA;
const _fmtKB = v => v != null ? `${(v / 1024).toFixed(1)} KB` : NA;
const _pctOrNA = (v, dp = 1) => v != null ? (v * 100).toFixed(dp) : NA;

function _row(l1, v1, l2 = '', v2 = '') { return { l1, v1, l2, v2 }; }
function _group(label) { return { group: label }; }

function renderKpiStrip(m) {
  const cloudOnly = isCloudOnly(m);
  const agg = isEdgeAgg(m);

  setText('kpi_lat_mean', m.latency_mean_ms != null ? m.latency_mean_ms.toFixed(1) : NA);
  setText('kpi_lat_p50', m.latency_p50_ms != null ? m.latency_p50_ms.toFixed(1) : NA);
  setText('kpi_lat_p95', m.latency_p95_ms != null ? m.latency_p95_ms.toFixed(1) : NA);
  setText('kpi_lat_p99', m.latency_p99_ms != null ? m.latency_p99_ms.toFixed(1) : NA);
  setText('kpi_delivery', _pctOrNA(m.e2e_unique_delivery_ratio));
  const dl = document.getElementById('kpi_delivery_label');
  if (dl) dl.textContent = 'Event Delivery %';

  const stateAgreement = { label: 'State Agreement', val: _pctOrNA(m.cloud_reflection_ratio), unit: 'final match' };

  let slots;
  if (cloudOnly) {
    slots = [
      stateAgreement,
      { label: 'Msgs to Cloud', val: _fmtInt(m.cloud_msgs_received), unit: 'received' },
      { label: 'Wireless Loss', val: _fmtInt(m.frames_s2e_dropped), unit: 'dropped' },
      { label: 'Retransmits', val: _fmtInt(m.proto_retransmissions), unit: 'protocol' },
    ];
  } else if (agg) {
    slots = [
      stateAgreement,
      { label: 'Msg Reduction', val: _pctOrNA(m.message_reduction_ratio), unit: '% saved' },
      { label: 'Aggregation', val: m.aggregation_ratio != null ? m.aggregation_ratio.toFixed(3) : NA, unit: 'frames/event' },
      { label: 'Backhaul Loss', val: _fmtInt(m.frames_e2c_dropped), unit: 'dropped' },
    ];
  } else {
    slots = [
      stateAgreement,
      { label: 'Msg Reduction', val: _pctOrNA(m.message_reduction_ratio), unit: '% saved' },
      { label: 'Filtered', val: _fmtInt(m.events_filtered_total), unit: 'events' },
      { label: 'Backhaul Loss', val: _fmtInt(m.frames_e2c_dropped), unit: 'dropped' },
    ];
  }

  slots.forEach((s, i) => {
    const idx = 5 + i;
    setText(`kpi_label_${idx}`, s.label);
    setText(`kpi_slot_${idx}`, s.val);
    setText(`kpi_unit_${idx}`, s.unit);
  });
}

function buildMetricsRows(m) {
  return isCloudOnly(m) ? _metricsCloudOnly(m) : _metricsEdge(m);
}

function _reliabilityGroup(m) {
  const applied = m.unique_state_changes_applied_at_cloud;
  const gen = m.state_changes_generated_total;
  const deliveryStr = (applied != null && gen)
    ? `${Number(applied).toLocaleString()} / ${Number(gen).toLocaleString()} (${_fmtPct(m.e2e_unique_delivery_ratio)})`
    : _fmtPct(m.e2e_unique_delivery_ratio);
  return [
    _group('Reliability'),
    _row('Event delivery — unique state changes applied / generated', deliveryStr,
         'First-pass physical survival', _fmtPct(m.physical_delivery_ratio)),
    _row('Final cloud state agreement', _fmtPct(m.cloud_reflection_ratio),
         'Duplicate events at cloud', _fmtInt(m.duplicate_events_at_cloud)),
  ];
}

function _metricsCloudOnly(m) {
  const snapshots = m.initial_snapshots_generated_total ?? 0;
  return [
    _group('Workload generation'),
    _row('Real arrivals/departures', _fmtInt(m.state_changes_generated_total), 'Initial occupancy snapshots', snapshots > 0 ? _fmtInt(snapshots) : '0  (empty-lot start)'),
    _row('Heartbeats emitted', _fmtInt(m.heartbeats_generated_total), 'Heartbeat interval', m.heartbeat_interval_s > 0 ? `${m.heartbeat_interval_s.toFixed(0)} s` : 'disabled'),
    _row('Duplicate sends', _fmtInt(m.duplicate_sends_generated_total), 'Total events emitted', _fmtInt(m.events_generated_total)),

    _group('Sensor link  (sensor → broker, direct)'),
    _row('Frames sent', _fmtInt(m.frames_s2e_sent), 'Frames lost on radio', _fmtInt(m.frames_s2e_dropped)),
    _row('Wireless delivery ratio', _fmtPct(m.s2e_delivery_ratio), 'Frames delivered to broker', _fmtInt(m.frames_s2e_delivered)),

    _group('Protocol  (sensor → cloud)'),
    _row('Retransmissions', _fmtInt(m.proto_retransmissions), 'Duplicate deliveries', _fmtInt(m.proto_duplicate_deliveries)),
    _row('Protocol overhead (KB)', _fmtKB(m.proto_bytes_sent), '', ''),

    _group('Cloud intake'),
    _row('Messages received (batches)', _fmtInt(m.cloud_batches_received), 'Events received (pre-dedup)', _fmtInt(m.cloud_msgs_received)),
    _row('Duplicate events removed', _fmtInt(m.duplicate_events_at_cloud), 'Unique events (post-dedup)', _fmtInt(m.cloud_events_post_dedup)),
    _row('Unique state changes applied', _fmtInt(m.unique_state_changes_applied_at_cloud), '', ''),

    ..._reliabilityGroup(m),

    _group('Latency  (sensor emit → cloud arrival, unique state changes)'),
    _row('Mean', _fmtV(m.latency_mean_ms, ' ms'), 'Min', _fmtV(m.latency_min_ms, ' ms')),
    _row('P50', _fmtV(m.latency_p50_ms, ' ms'), 'P95', _fmtV(m.latency_p95_ms, ' ms')),
    _row('P99', _fmtV(m.latency_p99_ms, ' ms'), 'Max', _fmtV(m.latency_max_ms, ' ms')),

    _group('Bandwidth per hop'),
    _row('Sensor → broker sent (KB)', _fmtKB(m.bytes_s2e_sent), 'Sensor → broker received (KB)', _fmtKB(m.bytes_s2e_received)),
    _row('Protocol bytes incl. overhead (KB)', _fmtKB(m.proto_bytes_sent), '', ''),
  ];
}

function _metricsEdge(m) {
  const agg = isEdgeAgg(m);
  const hbSuppressed = m.heartbeats_suppressed ?? 0;
  const qSuppressed = m.quarantine_suppressed ?? 0;
  const sameStateFiltered = Math.max(0, (m.events_filtered_total ?? 0) - hbSuppressed - qSuppressed);

  const rows = [
    _group('Workload generation'),
    _row('Real arrivals/departures', _fmtInt(m.state_changes_generated_total), 'Initial occupancy snapshots', (m.initial_snapshots_generated_total ?? 0) > 0 ? _fmtInt(m.initial_snapshots_generated_total) : '0  (empty-lot start)'),
    _row('Heartbeats emitted', _fmtInt(m.heartbeats_generated_total), 'Heartbeat interval', m.heartbeat_interval_s > 0 ? `${m.heartbeat_interval_s.toFixed(0)} s` : 'disabled'),
    _row('Duplicate sends', _fmtInt(m.duplicate_sends_generated_total), 'Total events emitted', _fmtInt(m.events_generated_total)),

    _group('Sensor link  (sensor → edge gateway)'),
    _row('Frames sent', _fmtInt(m.frames_s2e_sent), 'Frames lost on radio', _fmtInt(m.frames_s2e_dropped)),
    _row('Wireless delivery ratio', _fmtPct(m.s2e_delivery_ratio), 'Frames arrived at edge', _fmtInt(m.frames_s2e_delivered)),

    _group('Edge processing'),
    _row('Filtered (same-state / stale seq)', _fmtInt(sameStateFiltered), 'Heartbeats suppressed', _fmtInt(hbSuppressed)),
    _row('Quarantine suppressed', _fmtInt(qSuppressed), 'Total filtered', _fmtInt(m.events_filtered_total)),
    _row('Events forwarded to backhaul', _fmtInt(m.events_forwarded_total), 'Heartbeats forwarded (resync)', _fmtInt(m.heartbeats_forwarded)),
    _row('Message reduction', _fmtPct(m.message_reduction_ratio), '', ''),
  ];

  if (agg) {
    rows.push(
      _group('Aggregation'),
      _row('Batches (cloud frames)', _fmtInt(m.frames_e2c_sent), 'Avg events per batch', m.events_per_cloud_message != null ? m.events_per_cloud_message.toFixed(2) : NA),
      _row('Aggregation ratio', m.aggregation_ratio != null ? m.aggregation_ratio.toFixed(4) : NA, '', ''),
    );
  }

  rows.push(
    _group('Backhaul link  (edge → broker)'),
    _row('Frames sent', _fmtInt(m.frames_e2c_sent), 'Frames dropped (link + retransmit)', _fmtInt(m.frames_e2c_dropped)),
    _row('Frames delivered', _fmtInt(m.frames_e2c_delivered), 'Backhaul delivery ratio', _fmtPct(m.backhaul_delivery_ratio)),

    _group('Protocol  (edge → cloud)'),
    _row('Retransmissions', _fmtInt(m.proto_retransmissions), 'Duplicate deliveries', _fmtInt(m.proto_duplicate_deliveries)),
    _row('Protocol overhead (KB)', _fmtKB(m.proto_bytes_sent), '', ''),

    _group('Cloud intake'),
    _row('Messages received (batches)', _fmtInt(m.cloud_batches_received), 'Events received (pre-dedup)', _fmtInt(m.cloud_msgs_received)),
    _row('Duplicate events removed', _fmtInt(m.duplicate_events_at_cloud), 'Unique events (post-dedup)', _fmtInt(m.cloud_events_post_dedup)),
    _row('Unique state changes applied', _fmtInt(m.unique_state_changes_applied_at_cloud), '', ''),

    ..._reliabilityGroup(m),
  );

  if (hasAnomaly(m)) {
    rows.push(
      _group('Anomaly detection & adaptive mode'),
      _row('Anomalies detected', _fmtInt(m.anomalies_detected), 'Resolved', _fmtInt(m.anomalies_resolved)),
      _row('Active at end', _fmtInt(m.active_anomalies), 'Affected spots', _fmtInt(m.anomaly_detected_spots)),
      _row('Quarantined at end', _fmtInt(m.quarantined_spots_final), 'Quarantine suppressed msgs', _fmtInt(m.quarantine_suppressed)),
      _row('Adaptive mode switches', _fmtInt(m.adaptive_mode_switches), '', ''),
    );
    if ((m.fault_injected_count ?? 0) > 0) {
      rows.push(_row('Faults injected', _fmtInt(m.fault_injected_count), 'Faulty spots (ground truth)', _fmtInt(m.fault_true_count)));
      if (m.anomaly_precision != null) {
        rows.push(_row('Precision', _fmtPct(m.anomaly_precision), 'Recall', _fmtPct(m.anomaly_recall)));
        rows.push(_row('F1 Score', _fmtPct(m.anomaly_f1), '', ''));
      }
    }
  }

  rows.push(
    _group('Latency  (sensor emit → cloud arrival, unique state changes)'),
    _row('Mean', _fmtV(m.latency_mean_ms, ' ms'), 'Min', _fmtV(m.latency_min_ms, ' ms')),
    _row('P50', _fmtV(m.latency_p50_ms, ' ms'), 'P95', _fmtV(m.latency_p95_ms, ' ms')),
    _row('P99', _fmtV(m.latency_p99_ms, ' ms'), 'Max', _fmtV(m.latency_max_ms, ' ms')),

    _group('Bandwidth per hop'),
    _row('Sensor → edge sent (KB)', _fmtKB(m.bytes_s2e_sent), 'Sensor → edge received (KB)', _fmtKB(m.bytes_s2e_received)),
    _row('Edge → cloud sent (KB)', _fmtKB(m.bytes_e2c_sent), 'Edge → cloud received (KB)', _fmtKB(m.bytes_e2c_received)),
    _row('Protocol bytes incl. overhead (KB)', _fmtKB(m.proto_bytes_sent), '', ''),
  );

  return rows;
}

function renderMetricsTable(m) {
  const body = document.getElementById('metricsTableBody');
  if (!body) return;
  const rows = buildMetricsRows(m);
  body.innerHTML = rows.map(row => {
    if (row.group) {
      return `<tr class="metrics-group-header"><td colspan="4">${row.group}</td></tr>`;
    }
    return `<tr>
      <td>${row.l1}</td><td>${row.v1}</td>
      <td>${row.l2}</td><td>${row.v2}</td>
    </tr>`;
  }).join('');
}

function initCharts() {
  const make = (id, cfg) => {
    const el = document.getElementById(id);
    return el ? new Chart(el, cfg) : null;
  };

  charts.latencyHist = make('latencyHistChart', {
    type: 'bar',
    data: {
      labels: [],
      datasets: [{ label: 'Count', data: [], backgroundColor: C.cyan + '88', borderColor: C.cyan, borderWidth: 1 }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { title: { display: true, text: 'Latency (ms)', color: C.dim, font: { size: 9 } } },
        y: { title: { display: true, text: 'Count', color: C.dim, font: { size: 9 } } },
      },
    },
  });

  charts.msgCount = make('msgCountChart', {
    type: 'bar',
    data: {
      labels: ['Generated', 'Sent by sensor', 'Reached cloud', 'Filtered', 'Dropped'],
      datasets: [{
        data: [0, 0, 0, 0, 0], borderRadius: 4,
        backgroundColor: [C.purple + '88', C.blue + 'bb', C.cyan + 'bb', C.amber + '88', C.red + '88'],
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true } },
    },
  });

  charts.bandwidth = make('bandwidthChart', {
    type: 'doughnut',
    data: {
      labels: ['Sensor→Edge (KB)', 'Edge→Cloud (KB)'],
      datasets: [{
        data: [1, 0],
        backgroundColor: [C.blue + 'cc', C.cyan + 'cc'],
        borderColor: '#13161e', borderWidth: 3,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'bottom', labels: { font: { size: 10 }, padding: 8 } } },
    },
  });

  charts.comparison = make('comparisonChart', {
    type: 'bar',
    data: { labels: [], datasets: [] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { font: { size: 9 }, padding: 6 } } },
      scales: { y: { beginAtZero: true, title: { display: true, text: 'Mean Latency (ms)', color: C.dim, font: { size: 9 } } } },
    },
  });

  charts.delivery = make('deliveryChart', {
    type: 'bar',
    data: {
      labels: [],
      datasets: [{ label: 'Event Delivery %', data: [], backgroundColor: C.green + '88', borderColor: C.green, borderWidth: 1, borderRadius: 4 }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { min: 0, max: 100, title: { display: true, text: 'Event Delivery % (e2e_unique)', color: C.dim, font: { size: 9 } } } },
    },
  });
}

function updateLatencyHistogram(samples) {
  if (!charts.latencyHist || !samples?.length) return;
  const B = 18;
  const mn = Math.min(...samples);
  const mx = Math.max(...samples);
  const step = (mx - mn) / B || 1;
  const counts = Array(B).fill(0);
  const labels = [];
  for (let i = 0; i < B; i++) labels.push((mn + i * step).toFixed(0));
  samples.forEach(v => { counts[Math.min(Math.floor((v - mn) / step), B - 1)]++; });
  charts.latencyHist.data.labels = labels;
  charts.latencyHist.data.datasets[0].data = counts;
  charts.latencyHist.update('none');
}

function updateMsgCountChart(m) {
  if (!charts.msgCount) return;
  const cloudOnly = isCloudOnly(m);
  const sensorSent = m.frames_s2e_sent ?? 0;
  const atCloud = m.cloud_msgs_received ?? 0;
  const droppedTotal = (m.frames_s2e_dropped ?? 0) + (cloudOnly ? 0 : (m.frames_e2c_dropped ?? 0));

  const titleEl = document.getElementById('msgCountChartTitle');
  if (titleEl) titleEl.textContent = cloudOnly ? 'Message Flow — Sensor → Cloud' : 'Message Flow';

  if (cloudOnly) {
    charts.msgCount.data.labels = ['Sent by sensor', 'Reached cloud', 'Dropped'];
    charts.msgCount.data.datasets[0].data = [sensorSent, atCloud, droppedTotal];
  } else {
    charts.msgCount.data.labels = ['Sent by sensor', 'Reached cloud', 'Filtered (edge)', 'Dropped (total)'];
    charts.msgCount.data.datasets[0].data = [sensorSent, atCloud, m.events_filtered_total ?? 0, droppedTotal];
  }
  charts.msgCount.update('none');
}

function updateBandwidthChart(m) {
  if (!charts.bandwidth) return;
  const cloudOnly = isCloudOnly(m);
  const s2eKB = m.bytes_s2e_sent != null ? +(m.bytes_s2e_sent / 1024).toFixed(2) : 0;
  if (cloudOnly) {
    charts.bandwidth.data.labels = ['Sensor bytes sent (KB)', 'N/A (no backhaul)'];
    charts.bandwidth.data.datasets[0].data = [s2eKB, 0];
  } else {
    const e2cKB = m.bytes_e2c_sent != null ? +(m.bytes_e2c_sent / 1024).toFixed(2) : 0;
    charts.bandwidth.data.labels = ['Sensor → Edge (KB)', 'Edge → Cloud (KB)'];
    charts.bandwidth.data.datasets[0].data = [s2eKB, e2cKB];
  }
  charts.bandwidth.update('none');
}

function updateComparisonChart() {
  if (!charts.comparison || !allResults.length) return;
  const groups = {};
  allResults.forEach(raw => {
    const r = normalizeMetrics(raw);
    const k = `${(r.protocol || '?').toUpperCase()} / ${r.architecture || ''}`;
    if (!groups[k]) groups[k] = {};
    groups[k][r.traffic_level] = r.latency_mean_ms;
  });
  const levels = ['low', 'medium', 'peak'];
  const keys = Object.keys(groups);
  const pal = [C.cyan, C.blue, C.amber, C.purple, C.green, C.red];
  charts.comparison.data.labels   = levels;
  charts.comparison.data.datasets = keys.map((k, i) => ({
    label: k,
    data: levels.map(l => groups[k][l] ?? null),
    backgroundColor: pal[i % pal.length] + '99',
    borderColor: pal[i % pal.length],
    borderWidth: 1, borderRadius: 4,
  }));
  charts.comparison.update('none');
}

function updateDeliveryChart() {
  if (!charts.delivery || !allResults.length) return;
  const norm = allResults.map(normalizeMetrics);
  const labels = norm.map(m => (m.scenario_name || '').replace(/_/g, ' ').substring(0, 20));
  const data = norm.map(m => m.e2e_unique_delivery_ratio != null ? +(m.e2e_unique_delivery_ratio * 100).toFixed(1) : null);
  const colors = data.map(v => v == null ? C.dim + '55' : v >= 98 ? C.green + '99' : v >= 90 ? C.amber + '99' : C.red + '99');
  charts.delivery.data.labels = labels;
  charts.delivery.data.datasets[0].data = data;
  charts.delivery.data.datasets[0].backgroundColor = colors;
  charts.delivery.update('none');
}

function aiSetOutput(html) {
  const el = document.getElementById('aiOutput');
  if (el) el.innerHTML = html;
}

function aiSetTag(text, cls) {
  const el = document.getElementById('aiModelTag');
  if (!el) return;
  el.textContent = text;
  el.className = cls ? `ai-model-tag ${cls}` : 'ai-model-tag';
}

async function runAIAnalysis() {
  if (!allResults.length) {
    aiSetOutput('<p class="ai-error">⚠️ No results yet — run at least one scenario first.</p>');
    return;
  }
  const focus = document.getElementById('aiFocus')?.value ?? 'general';
  const scope = document.getElementById('aiScope')?.value ?? 'all';
  const btn = document.getElementById('aiAnalyseBtn');

  const toSend = scope === 'this' ? [currentResult ?? allResults[allResults.length - 1]]
    : scope === 'last3' ? allResults.slice(-3)
    : scope === 'last1' ? allResults.slice(-1)
    : allResults.slice();
  const stripped = toSend.map(r => {
    const c = { ...r }; delete c.latency_samples; delete c.scenario_log; return c;
  });

  if (btn) { btn.disabled = true; btn.textContent = '⏳ Thinking…'; }
  const thinkingLabel = stripped.length === 1
    ? `Analysing “${stripped[0].scenario_name || 'this run'}”…`
    : `Analysing ${stripped.length} runs…`;
  aiSetOutput(`<div class="ai-thinking"><span class="ai-spinner"></span> ${thinkingLabel}</div>`);
  aiSetTag('', '');

  try {
    const resp = await fetch('/api/interpret', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ results: stripped, focus }),
    });
    const data = await resp.json().catch(() => null);
    if (!resp.ok || !data) throw new Error(data?.detail ?? `Server error ${resp.status}`);
    if (!data.interpretation) throw new Error('Server returned empty interpretation');
    let html = renderMarkdown(data.interpretation);
    if (data.warning) html = `<div class="ai-warning">⚠️ ${data.warning}</div>` + html;
    aiSetOutput(html);
    aiSetTag(data.powered_by || data.model || '', data.model === 'rule-based' ? 'tag-builtin' : 'tag-groq');
  } catch (err) {
    console.error('[AI]', err);
    aiSetOutput(`<div class="ai-error">⚠️ ${err.message}</div>`);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '✦ Analyse'; }
  }
}

function renderMarkdown(text) {
  if (!text) return '';
  return text
    .replace(/^### (.+)$/gm, '<h4>$1</h4>')
    .replace(/^## (.+)$/gm, '<h3>$1</h3>')
    .replace(/^# (.+)$/gm, '<h2>$1</h2>')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/^[-*] (.+)$/gm, '<li>$1</li>')
    .replace(/(<li>.*<\/li>\n?)+/g, m => `<ul>${m}</ul>`)
    .replace(/^\d+\.\s(.+)$/gm, '<li>$1</li>')
    .replace(/\n\n+/g, '</p><p>')
    .replace(/^/, '<p>').replace(/$/, '</p>')
    .replace(/<p>\s*<\/p>/g, '')
    .replace(/<p>(<[hul])/g, '$1')
    .replace(/(<\/[hul][^>]*>)<\/p>/g, '$1');
}