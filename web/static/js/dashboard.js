Chart.defaults.color = '#8b91a8';
Chart.defaults.borderColor = '#2a2f45';
Chart.defaults.font.family = "'DM Sans', sans-serif";

const C = { cyan:'#00e5c8', blue:'#4d9bff', amber:'#ffc246', red:'#ff5252', green:'#39e887', purple:'#b47cff', dim:'#555a72', };

let charts = {};
let allResults = [];
let simRunning = false;
let currentSpotCount = 0;
let allScenarios = [];
let currentArchitecture = null;  

document.addEventListener('DOMContentLoaded', async () => {
  initCharts();
  await Promise.all([loadScenarios()]);
  connectSSE();
  await loadSavedResults();
});

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
      ? `${(full.protocol || '').toUpperCase()} · ${full.architecture} · ${full.traffic_level} · ${full.num_spots} spots · ${(full.sim_duration_s / 3600).toFixed(1)} h`
      : '');
    const hint = document.getElementById('groupHint');
    if (hint && full?.group) {
      const sibs = allScenarios.filter(s => s.group === full.group);
      hint.textContent = sibs.length > 1 ? `💡 Part of "${full.group}" - run all ${sibs.length} to compare` : '';
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
    method:  'POST',
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined
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
    const isCloudOnly = currentArchitecture === 'cloud_only';
    const fi = document.getElementById('progFilteredItem');
    const ai = document.getElementById('progAnomaliesItem');
    if (fi) fi.style.display = isCloudOnly ? 'none' : '';
    if (ai) ai.style.display = isCloudOnly ? 'none' : '';
  });
  es.addEventListener('progress', e => onProgress(JSON.parse(e.data)));
  es.addEventListener('sim_flushing', e => {
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
  setText('progSimTime', `${simH}h ${String(simM).padStart(2,'0')}m`);
  setText('progGenerated', d.generated ?? '-');
  setText('progHeartbeats', d.heartbeats ?? '-');
  setText('progReceived', d.cloud_events ?? '-');
  const occ = d.occupancy || {};
  const edge = d.edge || {};
  setText('progOccupancy', occ.occupancy_pct != null ? occ.occupancy_pct + '%' : '-');
  setText('progFiltered', edge.filtered ?? '-');
  const sensorDrops = edge.sensor_link_stats?.dropped ?? '-';
  const backhaulDrops = edge.link_stats?.dropped ?? '-';
  setText('progSensorDropped', sensorDrops);
  setText('progBackhaulDropped', backhaulDrops);
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
  ['progElapsed','progGenerated','progHeartbeats','progReceived','progOccupancy','progFiltered','progDropped','progAnomalies']
    .forEach(id => setText(id, '-'));
}

const _fmt = (v, unit = '', d = 1) => v != null ? `${(+v).toFixed(d)}${unit}` : '-';
const _fmtInt = v => (v != null ? String(v) : '-');
const _fmtPct = v => v != null ? `${(v * 100).toFixed(2)}%` : '-';
const _fmtKB  = v => v ? `${(v / 1024).toFixed(1)} KB` : '-';

function renderKpiStrip(r) {
  const isCloudOnly = r.architecture === 'cloud_only';

  setText('kpi_lat_mean', r.latency_mean_ms?.toFixed(1) ?? '-');
  setText('kpi_lat_p50', r.latency_p50_ms?.toFixed(1) ?? '-');
  setText('kpi_lat_p95', r.latency_p95_ms?.toFixed(1) ?? '-');
  setText('kpi_lat_p99', r.latency_p99_ms?.toFixed(1) ?? '-');
  
  const delivery = isCloudOnly ? (r.sensor_to_edge_delivery_ratio ?? 1) : (r.cloud_reflection_ratio ?? r.sensor_to_edge_delivery_ratio ?? 1);
  setText('kpi_delivery', (delivery * 100).toFixed(1));

  const slots = isCloudOnly ? [
    { label: 'Cloud Msgs', val: _fmtInt(r.events_reflected_in_cloud), unit: 'received'  },
    { label: 'Sensor Drops', val: _fmtInt(r.sensor_link_dropped), unit: 'pkts lost' },
    { label: 'Sensor Bytes', val: r.sensor_to_edge_bytes ? (r.sensor_to_edge_bytes / 1024).toFixed(1) : '-', unit: 'KB sent' },
    { label: 'Cloud Bytes', val: r.edge_to_cloud_bytes ? (r.edge_to_cloud_bytes  / 1024).toFixed(1) : '-', unit: 'KB recv' },
  ] : (() => {
    const isAgg = r.architecture === 'edge_aggregated';
    return [
      isAgg
        ? { label: 'Batch Size', val: r.events_per_cloud_message != null ? (+r.events_per_cloud_message).toFixed(1) : '-', unit: 'ev/msg' }
        : { label: 'Msg Saved', val: r.message_reduction_ratio != null ? (r.message_reduction_ratio * 100).toFixed(1) : '-', unit: '% saved' },
      { label: 'Filtered', val: _fmtInt(r.filtered_events), unit: 'events' },
      { label: 'S→E Bytes', val: r.sensor_to_edge_bytes ? (r.sensor_to_edge_bytes / 1024).toFixed(1) : '-', unit: 'KB' },
      { label: 'E→C Bytes', val: r.edge_to_cloud_bytes  ? (r.edge_to_cloud_bytes  / 1024).toFixed(1) : '-', unit: 'KB' },
    ];
  })();

  slots.forEach((s, i) => {
    const idx = 4 + i;
    setText(`kpi_label_${idx}`, s.label);
    setText(`kpi_slot_${idx}`, s.val);
    setText(`kpi_unit_${idx}`, s.unit);
  });
}


function metricsRowsCloudOnly(r) {
  const survivedWireless = (r.sensor_to_edge_msgs ?? 0) - (r.sensor_link_dropped ?? 0);
  const captured = Math.round((r.cloud_reflection_ratio ?? 0) * (r.valid_state_changes ?? 0));
  const total = r.valid_state_changes ?? 0;
  const coverage = total > 0 ? `${captured} of ${total} (${_fmtPct(r.cloud_reflection_ratio)})` : '-';

  return [
    { group: 'Sensor activity' },
    { l1: 'Sensor messages emitted', v1: _fmtInt(r.events_generated), l2: 'Real arrivals & departures', v2: _fmtInt(r.valid_state_changes) },
    { l1: 'Heartbeats emitted', v1: _fmtInt(r.heartbeats_generated),
      l2: 'Heartbeat interval', v2: (r.heartbeat_interval_s && r.heartbeat_interval_s > 0) ? `${r.heartbeat_interval_s.toFixed(0)} s` : 'disabled' },
    { l1: 'Duplicates generated', v1: _fmtInt(r.duplicate_sends_generated), l2: '', v2: '' },

    { group: 'Wireless link (sensor → broker)' },
    { l1: 'Unique messages sent', v1: _fmtInt(r.sensor_to_edge_msgs),
      l2: 'Lost on the radio', v2: _fmtInt(r.sensor_link_dropped) },
    { l1: 'Wireless delivery', v1: _fmtPct(r.sensor_to_edge_delivery_ratio),
      l2: 'Survived to broker', v2: _fmtInt(survivedWireless) },
    { l1: 'Link loss rate',
      v1: r.sensor_to_edge_delivery_ratio != null ? ((1 - r.sensor_to_edge_delivery_ratio) * 100).toFixed(2) + ' %' : '-',
      l2: 'Broker-layer delivery', v2: _fmtPct(r.edge_to_cloud_delivery_ratio) },

    { group: 'Broker layer (MQTT/AMQP/CoAP)' },
    { l1: 'Reached cloud', v1: _fmtInt(r.events_reflected_in_cloud),
      l2: 'Duplicate deliveries', v2: _fmtInt(r.duplicate_deliveries) },
    { l1: 'Broker retries (QoS)', v1: _fmtInt(r.retransmissions_total),
      l2: 'Protocol overhead', v2: _fmtKB(r.protocol_bytes) },

    { group: 'Coverage' },
    { l1: 'Real events captured', v1: coverage, l2: '', v2: '' },
 
    { group: 'End-to-End Latency' },
    { l1: 'Mean', v1: _fmt(r.latency_mean_ms, ' ms'),
      l2: 'Min', v2: _fmt(r.latency_min_ms, ' ms') },
    { l1: 'P50', v1: _fmt(r.latency_p50_ms, ' ms'),
      l2: 'P95', v2: _fmt(r.latency_p95_ms, ' ms') },
    { l1: 'P99', v1: _fmt(r.latency_p99_ms, ' ms'),
      l2: 'Max', v2: _fmt(r.latency_max_ms, ' ms') },

    { group: 'Bandwidth' },
    { l1: 'Sent by sensors', v1: _fmtKB(r.sensor_to_edge_bytes), l2: 'Reached broker', v2: _fmtKB(r.edge_to_cloud_bytes) },

  ];
}

function metricsRowsEdge(r) {
  const survivedWireless = (r.sensor_to_edge_msgs ?? 0) - (r.sensor_link_dropped ?? 0);
  const captured = Math.round((r.cloud_reflection_ratio ?? 0) * (r.valid_state_changes ?? 0));
  const total = r.valid_state_changes ?? 0;
  const coverage = total > 0 ? `${captured} of ${total} (${_fmtPct(r.cloud_reflection_ratio)})` : '-';

  const isAggregated = r.architecture === 'edge_aggregated';
  const edgeRows = [
    { l1: 'Filtered as redundant', v1: _fmtInt(r.filtered_events),
      l2: 'Forwarded to broker', v2: _fmtInt(r.edge_to_cloud_msgs) },
    { l1: 'Cloud traffic saved', v1: _fmtPct(r.message_reduction_ratio),
      l2: '', v2: '' },
  ];
  if (isAggregated) {
    edgeRows[1].l2 = 'Avg events per batch';
    edgeRows[1].v2 = _fmt(r.events_per_cloud_message, '', 2);
  }

  const hasAnomaly = (r.anomalies_detected ?? 0) > 0 || (r.adaptive_mode_switches ?? 0) > 0 || (r.quarantined_spots_final ?? 0) > 0;
  const anomalyRows = hasAnomaly ? [
    { group: 'Anomaly Detection' },
    { l1: 'Detected', v1: _fmtInt(r.anomalies_detected),
      l2: 'Resolved', v2: _fmtInt(r.anomalies_resolved) },
    { l1: 'Active at end', v1: _fmtInt(r.active_anomalies),
      l2: 'Affected spots', v2: _fmtInt(r.anomaly_detected_spots) },
    { l1: 'Quarantined at end', v1: _fmtInt(r.quarantined_spots_final),
      l2: 'Mode switches', v2: _fmtInt(r.adaptive_mode_switches) },
    ...((r.fault_injected_count ?? 0) > 0 ? [{ l1: 'Faults injected', v1: _fmtInt(r.fault_injected_count), l2: '', v2: '' }] : []),
  ] : [];
  if ((r.fault_injected_count ?? 0) > 0) {
    anomalyRows.push({ l1: 'Faults injected', v1: _fmtInt(r.fault_injected_count), l2: '', v2: '' });
  }

  return [
    { group: 'Sensor activity' },
    { l1: 'Sensor messages emitted', v1: _fmtInt(r.events_generated), l2: 'Real arrivals & departures', v2: _fmtInt(r.valid_state_changes) },
    { l1: 'Heartbeats emitted', v1: _fmtInt(r.heartbeats_generated), l2: 'Heartbeat interval', v2: (r.heartbeat_interval_s && r.heartbeat_interval_s > 0) ? `${r.heartbeat_interval_s.toFixed(0)} s` : 'disabled' },
    { l1: 'Heartbeats forwarded', v1: _fmtInt(r.heartbeats_forwarded), l2: 'Duplicates generated', v2: _fmtInt(r.duplicate_sends_generated) },

    { group: 'Wireless link (sensor → edge gateway)' },
    { l1: 'Unique messages sent', v1: _fmtInt(r.sensor_to_edge_msgs), l2: 'Lost on the radio', v2: _fmtInt(r.sensor_link_dropped) },
    { l1: 'Wireless delivery', v1: _fmtPct(r.sensor_to_edge_delivery_ratio), l2: 'Reached the gateway', v2: _fmtInt(survivedWireless) },
    { l1: 'Sensor link loss', v1: r.sensor_to_edge_delivery_ratio != null ? ((1 - r.sensor_to_edge_delivery_ratio) * 100).toFixed(2) + ' %' : '-', l2: '', v2: '' },

    { group: 'Edge processing' },
    ...edgeRows,

    { group: 'Backhaul link (edge → broker)' },
    { l1: 'Messages on backhaul', v1: _fmtInt(r.edge_to_cloud_msgs), l2: 'Lost on backhaul', v2: _fmtInt(r.edge_to_cloud_dropped) },
    { l1: 'Backhaul delivery', v1: _fmtPct(r.edge_to_cloud_delivery_ratio), l2: 'Broker retries (QoS)', v2: _fmtInt(r.retransmissions_total) },

    { group: 'Coverage' },
    { l1: 'Real events captured', v1: coverage, l2: 'Reached cloud (total)', v2: _fmtInt(r.events_reflected_in_cloud) },

    { group: 'End-to-End Latency' },
    { l1: 'Mean', v1: _fmt(r.latency_mean_ms, ' ms'), l2: 'Min', v2: _fmt(r.latency_min_ms, ' ms') },
    { l1: 'P50', v1: _fmt(r.latency_p50_ms, ' ms'), l2: 'P95', v2: _fmt(r.latency_p95_ms, ' ms') },
    { l1: 'P99', v1: _fmt(r.latency_p99_ms, ' ms'), l2: 'Max', v2: _fmt(r.latency_max_ms, ' ms') },

    ...anomalyRows,

    { group: 'Bandwidth' },
    { l1: 'Sensor → edge', v1: _fmtKB(r.sensor_to_edge_bytes), l2: 'Edge → broker', v2: _fmtKB(r.edge_to_cloud_bytes) },
    { l1: 'Protocol overhead', v1: _fmtKB(r.protocol_bytes), l2: '', v2: '' },
  ];
}

function renderMetricsTable(r) {
  const body = document.getElementById('metricsTableBody');
  if (!body) return;
  const rows = r.architecture === 'cloud_only'
    ? metricsRowsCloudOnly(r)
    : metricsRowsEdge(r);

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
  const lbl = document.getElementById('metricsToggleLabel');
  const open = wrap.classList.toggle('open');
  lbl.textContent = open ? '▲ Full Metrics' : '▼ Full Metrics';
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
  row.innerHTML = `
    <div class="result-name" title="${r.scenario_name}">${r.scenario_name}</div>
    <div class="result-meta">
      ${historicalBadge}
      <span class="result-tag ${protoClass}">${(r.protocol || '').toUpperCase()}</span>
      <span class="result-tag">${r.architecture || ''}</span>
      <span class="result-tag">${r.traffic_level || ''}</span>
      ${durationLabel ? `<span class="result-tag">${durationLabel}</span>` : ''}
      <span class="result-lat">${r.latency_mean_ms?.toFixed(1) ?? '-'}ms</span>
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
    aiSetOutput('<p class="ai-error">⚠️ No results yet - run at least one scenario first.</p>');
    return;
  }
  const focus = document.getElementById('aiFocus')?.value ?? 'general';
  const scope = document.getElementById('aiScope')?.value ?? 'all';
  const btn = document.getElementById('aiAnalyseBtn');

  const toSend = scope === 'last3' ? allResults.slice(-3) : scope === 'last1' ? allResults.slice(-1) : allResults.slice();
  const stripped = toSend.map(r => { const c = { ...r }; delete c.latency_samples; return c; });

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
    aiSetTag(
      data.powered_by || data.model || '',
      data.model === 'rule-based' ? 'tag-builtin' : 'tag-groq',
    );
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

function initCharts() {
  const make = (id, cfg) => {
    const el = document.getElementById(id);
    return el ? new Chart(el, cfg) : null;
  };

  charts.latencyHist = make('latencyHistChart', {
    type: 'bar',
    data: { labels: [], datasets: [{ label: 'Count', data: [],
      backgroundColor: C.cyan + '88', borderColor: C.cyan, borderWidth: 1 }] },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { title: { display: true, text: 'Latency (ms)', color: C.dim, font: { size: 9 } } },
        y: { title: { display: true, text: 'Count', color: C.dim, font: { size: 9 } } },
      } },
  });

  charts.msgCount = make('msgCountChart', {
    type: 'bar',
    data: {
      labels: ['S→E Sent', 'E→C Msgs', 'Filtered', 'Dropped'],
      datasets: [{ data: [0, 0, 0, 0], borderRadius: 4,
        backgroundColor: [C.blue + 'bb', C.cyan + 'bb', C.amber + '88', C.red + '88'] }],
    },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } },
  });

  charts.bandwidth = make('bandwidthChart', {
    type: 'doughnut',
    data: {
      labels: ['Sensor→Edge (KB)', 'Edge→Cloud (KB)'],
      datasets: [{ data: [1, 0], backgroundColor: [C.blue + 'cc', C.cyan + 'cc'],
        borderColor: '#13161e', borderWidth: 3 }],
    },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'bottom', labels: { font: { size: 10 }, padding: 8 } } } },
  });

  charts.comparison = make('comparisonChart', {
    type: 'bar', data: { labels: [], datasets: [] },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { font: { size: 9 }, padding: 6 } } },
      scales: { y: { beginAtZero: true,
        title: { display: true, text: 'Mean Latency (ms)', color: C.dim, font: { size: 9 } } } } },
  });

  charts.delivery = make('deliveryChart', {
    type: 'bar',
    data: { labels: [], datasets: [{ label: 'Delivery %', data: [],
      backgroundColor: C.green + '88', borderColor: C.green, borderWidth: 1, borderRadius: 4 }] },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { min: 0, max: 100,
        title: { display: true, text: 'Delivery %', color: C.dim, font: { size: 9 } } } } },
  });
}

function updateLatencyHistogram(samples) {
  if (!charts.latencyHist || !samples?.length) return;
  const B = 18, mn = Math.min(...samples), mx = Math.max(...samples);
  const step = (mx - mn) / B || 1, counts = Array(B).fill(0), labels = [];
  for (let i = 0; i < B; i++) labels.push((mn + i * step).toFixed(0));
  samples.forEach(v => { counts[Math.min(Math.floor((v - mn) / step), B - 1)]++; });
  charts.latencyHist.data.labels = labels;
  charts.latencyHist.data.datasets[0].data = counts;
  charts.latencyHist.update('none');
}


function updateMsgCountChart(r) {
  if (!charts.msgCount) return;
  const isCloudOnly = r.architecture === 'cloud_only';
  const s2e = r.sensor_to_edge_msgs || 0;
  const dropped = Math.round(s2e * (1 - (r.sensor_to_edge_delivery_ratio || 1)));
  const backhaulDrops = r.edge_to_cloud_dropped || 0;
  const titleEl = document.getElementById('msgCountChartTitle');

  if (isCloudOnly) {
    if (titleEl) titleEl.textContent = 'Message Counts - Sensor → Cloud';
    charts.msgCount.data.labels = ['Sensor Sent', 'Cloud Received', 'Wireless Dropped'];
    charts.msgCount.data.datasets[0].data = [s2e, r.events_reflected_in_cloud || 0, dropped];
    charts.msgCount.data.datasets[0].backgroundColor = [C.blue + 'bb', C.cyan + 'bb', C.red + '88'];
  } else {
    if (titleEl) titleEl.textContent = 'Message Counts by Link';
    charts.msgCount.data.labels = ['S→E Sent', 'E→C Msgs', 'Filtered', 'Wireless Drops', 'Backhaul Drops'];
    charts.msgCount.data.datasets[0].data = [s2e, r.edge_to_cloud_msgs || 0, r.filtered_events || 0, dropped, backhaulDrops];
    charts.msgCount.data.datasets[0].backgroundColor = [C.blue + 'bb', C.cyan + 'bb', C.amber + '88', C.red + '88', C.red + 'cc'];
  }
  charts.msgCount.update('none');
}

function updateBandwidthChart(r) {
  if (!charts.bandwidth) return;
  const isCloudOnly = r.architecture === 'cloud_only';
  charts.bandwidth.data.labels = isCloudOnly ? ['Sensor → Cloud sent (KB)', 'Cloud received (KB)'] : ['Sensor → Edge (KB)', 'Edge → Cloud (KB)'];
  charts.bandwidth.data.datasets[0].data = [
    +((r.sensor_to_edge_bytes || 0) / 1024).toFixed(2),
    +((r.edge_to_cloud_bytes || 0) / 1024).toFixed(2),
  ];
  charts.bandwidth.update('none');
}

function updateComparisonChart() {
  if (!charts.comparison || !allResults.length) return;
  const groups = {};
  allResults.forEach(r => {
    const k = `${(r.protocol || '?').toUpperCase()}\n${r.architecture || ''}`;
    if (!groups[k]) groups[k] = {};
    groups[k][r.traffic_level] = r.latency_mean_ms;
  });
  const levels = ['low', 'medium', 'peak'], keys = Object.keys(groups);
  const pal = [C.cyan, C.blue, C.amber, C.purple, C.green, C.red];
  charts.comparison.data.labels = levels;
  charts.comparison.data.datasets = keys.map((k, i) => ({
    label: k.replace('\n', ' / '),
    data: levels.map(l => groups[k][l] ?? null),
    backgroundColor: pal[i % pal.length] + '99',
    borderColor: pal[i % pal.length],
    borderWidth: 1, borderRadius: 4,
  }));
  charts.comparison.update('none');
}

function updateDeliveryChart() {
  if (!charts.delivery || !allResults.length) return;
  const labels = allResults.map(r => r.scenario_name?.replace(/_/g, ' ').substring(0, 20));
  const data = allResults.map(r => {
    let v = r.final_state_match_ratio;
    if (v == null || v === 0) 
      v = r.architecture === 'cloud_only' ? (r.sensor_to_edge_delivery_ratio ?? 1) : (r.cloud_reflection_ratio ?? r.sensor_to_edge_delivery_ratio ?? 1);
    return +(v * 100).toFixed(1);
  });
  const colors = data.map(v => v >= 98 ? C.green + '99' : v >= 90 ? C.amber + '99' : C.red + '99');
  charts.delivery.data.labels = labels;
  charts.delivery.data.datasets[0].data = data;
  charts.delivery.data.datasets[0].backgroundColor = colors;
  if (charts.delivery.options?.plugins?.title) 
    charts.delivery.options.plugins.title.text = 'Real-state match at cloud (% of spots whose state matches ground truth)';
  charts.delivery.update('none');
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
    note.style.cssText = 'grid-column:1/-1;font-size:.65rem;color:var(--dim);text-align:center;padding:.25rem 0';
    note.textContent = `Showing first 500 of ${n} spots`;
    grid.appendChild(note);
  }
  const ls = document.getElementById('lotStats');
  if (ls) ls.style.display = 'grid';
  setText('lst_total', n); setText('lst_free', n);
  setText('lst_occ', 0);   setText('lst_pct', '0%');
}

function updateSpot(id, state) {
  const el = document.getElementById(`spot_${id}`);
  if (!el) return;
  const s = String(state);
  if (el.dataset.state === s) return;
  el.dataset.state = s;
  el.className = `spot ${s} flash`;
}

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val ?? '-';
}

async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`fetchJSON ${url} → ${r.status}`);
  return r.json();
}