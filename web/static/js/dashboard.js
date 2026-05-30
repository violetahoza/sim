Chart.defaults.color = '#8b91a8';
Chart.defaults.borderColor = '#2a2f45';
Chart.defaults.font.family = "'DM Sans', sans-serif";

const C = { cyan: '#00e5c8', blue: '#4d9bff', amber: '#ffc246', red: '#ff5252', green: '#39e887', purple: '#b47cff', dim: '#555a72'};

let charts = {};
let allResults = [];
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
    showResult(results[results.length - 1], false);
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
    buildLotGrid(d.num_spots || 50);
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
    showResult(r, true);
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

function buildLotGrid(n) {
  if (n === currentSpotCount) return;
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

function addResultRow(r, scrollIntoView) {
  const container = document.getElementById('resultsTable');
  if (!container) return;
  container.querySelector('.results-empty')?.remove();
  const row = document.createElement('div');
  row.className = 'result-row';
  const protoClass = r.protocol === 'amqp' ? 'tag-amqp' : `tag-${r.protocol}`;
  const historicalBadge = r.source === 'historical' ? '<span class="result-tag tag-historical">saved</span>' : '';
  const durationLabel = r.sim_duration_s ? `${(r.sim_duration_s / 3600).toFixed(1)}h` : '';
  const logBadge = r.scenario_log?.length > 0 ? '<span class="result-tag" style="color:var(--purple)">log</span>' : '';
  row.innerHTML = `
    <div class="result-name" title="${r.scenario_name}">${r.scenario_name}</div>
    <div class="result-meta">
      ${historicalBadge}
      <span class="result-tag ${protoClass}">${(r.protocol || '').toUpperCase()}</span>
      <span class="result-tag">${r.architecture || ''}</span>
      <span class="result-tag">${r.traffic_level || ''}</span>
      ${durationLabel ? `<span class="result-tag">${durationLabel}</span>` : ''}
      ${logBadge}
      <span class="result-lat">${r.latency_mean_ms?.toFixed(1) ?? '—'}ms</span>
    </div>`;
  row.onclick = () => {
    document.querySelectorAll('.result-row').forEach(rr => rr.classList.remove('active'));
    row.classList.add('active');
    showResult(r, false);
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

function showResult(r, animate) {
  renderKpiStrip(r);
  renderMetricsTable(r);
  updateLatencyHistogram(r.latency_samples || []);
  updateMsgCountChart(r);
  updateBandwidthChart(r);
  if (animate) buildLotGrid(r.num_spots);
}

function toggleMetricsTable() {
  const wrap = document.getElementById('metricsTableWrap');
  const lbl  = document.getElementById('metricsToggleLabel');
  const open = wrap.classList.toggle('open');
  if (lbl) lbl.textContent = open ? '▲ Full Metrics' : '▼ Full Metrics';
}

function renderKpiStrip(r) {
  const cloudOnly = isCloudOnly(r);
  const agg = isEdgeAgg(r);

  setText('kpi_lat_mean', r.latency_mean_ms?.toFixed(1) ?? '—');
  setText('kpi_lat_p50', r.latency_p50_ms?.toFixed(1) ?? '—');
  setText('kpi_lat_p95', r.latency_p95_ms?.toFixed(1) ?? '—');
  setText('kpi_lat_p99', r.latency_p99_ms?.toFixed(1) ?? '—');
  setText('kpi_delivery', ((r.cloud_reflection_ratio ?? 0) * 100).toFixed(1));

  let slots;
  if (cloudOnly) {
    slots = [
      { label: 'Msgs to Cloud', val: _fmtInt(r.cloud_msgs_received_total ?? r.cloud_msgs_received), unit: 'received' },
      { label: 'Wireless Loss', val: _fmtInt(r.sensor_link_dropped), unit: 'dropped' },
      { label: 'Sensor Bytes', val: r.sensor_to_edge_bytes ? (r.sensor_to_edge_bytes / 1024).toFixed(1) : '—', unit: 'KB sent' },
      { label: 'Retransmits', val: _fmtInt(r.retransmissions_total), unit: 'QoS retries' },
    ];
  } else if (agg) {
    slots = [
      { label: 'Batch Size', val: r.events_per_cloud_message != null ? (+r.events_per_cloud_message).toFixed(1) : '—', unit: 'ev/msg' },
      { label: 'Msg Reduction', val: r.message_reduction_ratio != null ? (r.message_reduction_ratio * 100).toFixed(1) : '—', unit: '% saved' },
      { label: 'Filtered', val: _fmtInt(r.filtered_events), unit: 'events' },
      { label: 'Backhaul Loss', val: _fmtInt(r.edge_to_cloud_dropped), unit: 'dropped' },
    ];
  } else {
    slots = [
      { label: 'Msg Reduction', val: r.message_reduction_ratio != null ? (r.message_reduction_ratio * 100).toFixed(1) : '—', unit: '% saved' },
      { label: 'Filtered', val: _fmtInt(r.filtered_events), unit: 'events' },
      { label: 'S→E Bytes', val: r.sensor_to_edge_bytes ? (r.sensor_to_edge_bytes / 1024).toFixed(1) : '—', unit: 'KB' },
      { label: 'Backhaul Loss', val: _fmtInt(r.edge_to_cloud_dropped), unit: 'dropped' },
    ];
  }

  slots.forEach((s, i) => {
    const idx = 5 + i;
    setText(`kpi_label_${idx}`, s.label);
    setText(`kpi_slot_${idx}`, s.val);
    setText(`kpi_unit_${idx}`, s.unit);
  });
}

const _fmtV = (v, unit = '', dp = 1) => v != null ? `${(+v).toFixed(dp)}${unit}` : '—';
const _fmtInt = v => v != null ? Number(v).toLocaleString() : '—';
const _fmtPct = (v, dp = 2) => v != null ? `${(v * 100).toFixed(dp)}%` : '—';
const _fmtKB = v => v ? `${(v / 1024).toFixed(1)} KB` : '—';

function _row(l1, v1, l2 = '', v2 = '') { return { l1, v1, l2, v2 }; }
function _group(label) { return { group: label }; }

function buildMetricsRows(r) {
  return isCloudOnly(r) ? _metricsCloudOnly(r) : _metricsEdge(r);
}

function _metricsCloudOnly(r) {
  const wireDelivered = (r.sensor_to_edge_msgs ?? 0) - (r.sensor_link_dropped ?? 0);
  const cloudMsgsTotal = r.cloud_msgs_received_total ?? r.cloud_msgs_received ?? 0;
  const stateChangesReflected = r.cloud_state_changes_reflected ?? 0;
  const brokerDR = wireDelivered > 0 ? Math.min(cloudMsgsTotal / wireDelivered, 1.0) : null;

  const snapshots = r.initial_snapshots_generated ?? 0;
  const totalEmit = r.events_generated ?? 0;

  const coverageTotal = r.valid_state_changes ?? 0;
  const coverageStr = coverageTotal > 0
    ? `${stateChangesReflected.toLocaleString()} / ${coverageTotal.toLocaleString()} (${_fmtPct(r.cloud_reflection_ratio)})`
    : '—';

  return [
    _group('Workload generation'),
    _row('Real arrivals/departures', _fmtInt(r.valid_state_changes), 'Initial occupancy snapshots', snapshots > 0 ? _fmtInt(snapshots) : '0  (empty-lot start)'),
    _row('Heartbeats emitted', _fmtInt(r.heartbeats_generated), 'Heartbeat interval', r.heartbeat_interval_s > 0 ? `${r.heartbeat_interval_s.toFixed(0)} s` : 'disabled'),
    _row('Duplicate sends', _fmtInt(r.duplicate_sends_generated), 'Total msgs emitted', _fmtInt(totalEmit)),

    _group('Sensor link  (sensor → broker)'),
    _row('Frames sent', _fmtInt(r.sensor_to_edge_msgs), 'Frames lost on radio', _fmtInt(r.sensor_link_dropped)),
    _row('Wireless delivery ratio', _fmtPct(r.sensor_to_edge_delivery_ratio), 'Frames delivered to broker', _fmtInt(wireDelivered)),

    _group('Broker'),
    _row('QoS retransmits', _fmtInt(r.retransmissions_total), 'Duplicates caught by protocol layer', _fmtInt(r.duplicate_deliveries)),

    _group('Cloud intake'),
    _row('Total msgs received', _fmtInt(cloudMsgsTotal), 'Broker delivery ratio', brokerDR != null ? _fmtPct(brokerDR) : '1.000  (QoS 0)'),

    _group('Coverage  (state-change reflection)'),
    _row('State transitions reflected / generated', coverageStr, '', ''),

    _group('Latency  (sensor emit → cloud arrival)'),
    _row('Mean', _fmtV(r.latency_mean_ms, ' ms'), 'Min', _fmtV(r.latency_min_ms, ' ms')),
    _row('P50', _fmtV(r.latency_p50_ms, ' ms'), 'P95', _fmtV(r.latency_p95_ms, ' ms')),
    _row('P99', _fmtV(r.latency_p99_ms, ' ms'), 'Max', _fmtV(r.latency_max_ms, ' ms')),

    _group('Bandwidth'),
    _row('Sensor → broker (KB)', _fmtKB(r.sensor_to_edge_bytes), 'Protocol bytes incl. overhead (KB)', _fmtKB(r.protocol_bytes)),
  ];
}

function _metricsEdge(r) {
  const agg = isEdgeAgg(r);

  const wireDelivered = (r.sensor_to_edge_msgs ?? 0) - (r.sensor_link_dropped ?? 0);
  const bkhlSent = r.edge_to_cloud_msgs ?? 0;
  const bkhlDropped = r.edge_to_cloud_dropped ?? 0;
  const bkhlDelivered = bkhlSent - bkhlDropped;
  const cloudMsgsTotal = r.cloud_msgs_received_total ?? 0;
  const stateChangesReflected = r.cloud_state_changes_reflected ?? 0;

  const snapshots = r.initial_snapshots_generated ?? 0;
  const totalEmit = r.events_generated ?? 0;

  const hbSuppressed = r.heartbeats_suppressed ?? 0;
  const qSuppressed = r.quarantine_suppressed ?? 0;
  const sameStateFiltered = Math.max(0, (r.filtered_events ?? 0) - hbSuppressed - qSuppressed);

  const evtsPerBatch = r.events_per_cloud_message ?? 0;
  const evtsInBatches = agg ? Math.max(0, wireDelivered - (r.filtered_events ?? 0)) : 0;
  const evtsDelivered = agg ? Math.round(evtsPerBatch * bkhlDelivered) : cloudMsgsTotal;
  const evtsInDropped = agg ? Math.max(0, evtsInBatches - evtsDelivered) : 0;

  const coverageTotal = r.valid_state_changes ?? 0;
  const coverageStr = coverageTotal > 0
    ? `${stateChangesReflected.toLocaleString()} / ${coverageTotal.toLocaleString()} (${_fmtPct(r.cloud_reflection_ratio)})`
    : '—';

  const rows = [
    _group('Workload generation'),
    _row('Real arrivals/departures', _fmtInt(r.valid_state_changes), 'Initial occupancy snapshots', snapshots > 0 ? _fmtInt(snapshots) : '0  (empty-lot start)'),
    _row('Heartbeats emitted', _fmtInt(r.heartbeats_generated), 'Heartbeat interval', r.heartbeat_interval_s > 0 ? `${r.heartbeat_interval_s.toFixed(0)} s` : 'disabled'),
    _row('Duplicate sends', _fmtInt(r.duplicate_sends_generated), 'Total msgs emitted', _fmtInt(totalEmit)),

    _group('Sensor link  (sensor → edge gateway)'),
    _row('Frames sent', _fmtInt(r.sensor_to_edge_msgs), 'Frames lost on radio', _fmtInt(r.sensor_link_dropped)),
    _row('Wireless delivery ratio', _fmtPct(r.sensor_to_edge_delivery_ratio), 'Frames arrived at edge', _fmtInt(wireDelivered)),

    _group('Edge processing  (what happened to the ' + _fmtInt(wireDelivered) + ' arrived frames)'),
    _row('Filtered (same-state / stale seq)', _fmtInt(sameStateFiltered), 'Heartbeats suppressed', _fmtInt(hbSuppressed)),
    _row('Quarantine suppressed', _fmtInt(qSuppressed), 'Total filtered', _fmtInt(r.filtered_events)),
    _row('Message reduction', _fmtPct(r.message_reduction_ratio), '', ''),
  ];

  if (agg) {
    rows.push(
      _row('Events queued into batches', _fmtInt(evtsInBatches), 'Heartbeats forwarded via resync', _fmtInt(r.heartbeats_forwarded)),
      _row('Batches created  (wire messages)', _fmtInt(r.aggregation_batches_created ?? bkhlSent), 'Avg events per batch', _fmtV(evtsPerBatch, '', 2)),
      _row('Aggregation ratio', r.aggregation_ratio != null ? r.aggregation_ratio.toFixed(4) : '—', '', ''),

      _group('Backhaul link  (edge → broker)  —  messages'),
      _row('Batches sent', _fmtInt(bkhlSent), 'Batches lost', _fmtInt(bkhlDropped)),
      _row('Backhaul delivery ratio', _fmtPct(r.backhaul_delivery_ratio), 'Batches delivered', _fmtInt(bkhlDelivered)),

      _group('Backhaul link  —  events inside batches'),
      _row('Events in sent batches', _fmtInt(evtsInBatches), 'Events in delivered batches', _fmtInt(evtsDelivered)),
      _row('Events lost with dropped batches', _fmtInt(evtsInDropped), 'Event loss rate', evtsInBatches > 0 ? _fmtPct(evtsInDropped / evtsInBatches) : '0.00%'),
    );
  } else {
    const fwdRate = wireDelivered > 0 ? (bkhlSent / wireDelivered) : null;
    rows.push(
      _row('Forwarded to backhaul  (1 event / msg)', _fmtInt(bkhlSent), 'Forwarding rate', fwdRate != null ? _fmtPct(fwdRate) : '—'),
      _row('Heartbeats forwarded via resync', _fmtInt(r.heartbeats_forwarded), '', ''),

      _group('Backhaul link  (edge → broker)'),
      _row('Msgs sent', _fmtInt(bkhlSent), 'Msgs lost', _fmtInt(bkhlDropped)),
      _row('Backhaul delivery ratio', _fmtPct(r.backhaul_delivery_ratio), 'Msgs delivered', _fmtInt(bkhlDelivered)),
    );
  }

  rows.push(
    _group('Broker'),
    _row('QoS retransmits', _fmtInt(r.retransmissions_total), 'Duplicates caught by protocol layer', _fmtInt(r.duplicate_deliveries ?? 0)),

    _group('Cloud intake'),
    _row('Total msgs received', _fmtInt(cloudMsgsTotal), 'State transitions reflected / generated', coverageStr),
  );

  if (hasAnomaly(r)) {
    rows.push(
      _group('Anomaly detection & adaptive mode'),
      _row('Anomalies detected', _fmtInt(r.anomalies_detected), 'Resolved', _fmtInt(r.anomalies_resolved)),
      _row('Active at end', _fmtInt(r.active_anomalies), 'Affected spots', _fmtInt(r.anomaly_detected_spots)),
      _row('Quarantined at end', _fmtInt(r.quarantined_spots_final), 'Quarantine suppressed msgs', _fmtInt(r.quarantine_suppressed ?? 0)),
      _row('Adaptive mode switches', _fmtInt(r.adaptive_mode_switches), '', ''),
    );
    if ((r.fault_injected_count ?? 0) > 0)
      rows.push(_row('Faults injected', _fmtInt(r.fault_injected_count), '', ''));
  }

  rows.push(
    _group('Latency  (sensor emit → cloud arrival)'),
    _row('Mean', _fmtV(r.latency_mean_ms, ' ms'), 'Min', _fmtV(r.latency_min_ms, ' ms')),
    _row('P50', _fmtV(r.latency_p50_ms, ' ms'), 'P95', _fmtV(r.latency_p95_ms, ' ms')),
    _row('P99', _fmtV(r.latency_p99_ms, ' ms'), 'Max', _fmtV(r.latency_max_ms, ' ms')),

    _group('Bandwidth per hop'),
    _row('Sensor → edge  (KB)', _fmtKB(r.sensor_to_edge_bytes), 'Edge → cloud  (KB)', _fmtKB(r.edge_to_cloud_bytes ?? 0)),
    _row('Protocol bytes incl. overhead  (KB)', _fmtKB(r.protocol_bytes), '', ''),
  );

  return rows;
}

function renderMetricsTable(r) {
  const body = document.getElementById('metricsTableBody');
  if (!body) return;
  const rows = buildMetricsRows(r);
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
      datasets: [{ label: 'Cloud Reflection %', data: [], backgroundColor: C.green + '88', borderColor: C.green, borderWidth: 1, borderRadius: 4 }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { min: 0, max: 100, title: { display: true, text: 'Cloud Reflection %', color: C.dim, font: { size: 9 } } } },
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

function updateMsgCountChart(r) {
  if (!charts.msgCount) return;
  const cloudOnly = isCloudOnly(r);
  const sensorSent = r.sensor_to_edge_msgs ?? 0;
  const atCloud = r.cloud_msgs_received_total ?? r.cloud_msgs_received ?? 0;
  const filtered = cloudOnly ? 0 : (r.filtered_events ?? 0);
  const totalDropped = (r.sensor_link_dropped ?? 0) + (cloudOnly ? 0 : (r.edge_to_cloud_dropped ?? 0));

  const titleEl = document.getElementById('msgCountChartTitle');
  if (titleEl) titleEl.textContent = cloudOnly ? 'Message Flow — Sensor → Cloud' : 'Message Flow';

  charts.msgCount.data.labels = ['Sent by sensor', 'Reached cloud', 'Filtered (edge)', 'Dropped (total)'];
  charts.msgCount.data.datasets[0].data = [sensorSent, atCloud, filtered, totalDropped];
  charts.msgCount.update('none');
}

function updateBandwidthChart(r) {
  if (!charts.bandwidth) return;
  const cloudOnly = isCloudOnly(r);
  charts.bandwidth.data.labels = cloudOnly ? ['Sensor bytes sent (KB)', 'N/A (no backhaul)'] : ['Sensor → Edge (KB)', 'Edge → Cloud (KB)'];
  charts.bandwidth.data.datasets[0].data = [+((r.sensor_to_edge_bytes ?? 0) / 1024).toFixed(2), cloudOnly ? 0 : +((r.edge_to_cloud_bytes ?? 0) / 1024).toFixed(2)];
  charts.bandwidth.update('none');
}

function updateComparisonChart() {
  if (!charts.comparison || !allResults.length) return;
  const groups = {};
  allResults.forEach(r => {
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
  const labels = allResults.map(r => (r.scenario_name || '').replace(/_/g, ' ').substring(0, 20));
  const data = allResults.map(r => +((r.cloud_reflection_ratio ?? 1) * 100).toFixed(1));
  const colors = data.map(v => v >= 98 ? C.green + '99' : v >= 90 ? C.amber + '99' : C.red + '99');
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

  const toSend = scope === 'last3' ? allResults.slice(-3) : scope === 'last1' ? allResults.slice(-1) : allResults.slice();
  const stripped = toSend.map(r => {
    const c = { ...r }; delete c.latency_samples; delete c.scenario_log; return c;
  });

  if (btn) { btn.disabled = true; btn.textContent = '⏳ Thinking…'; }
  aiSetOutput(`<div class="ai-thinking"><span class="ai-spinner"></span> Analysing ${stripped.length} run${stripped.length !== 1 ? 's' : ''}…</div>`);
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