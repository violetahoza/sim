Chart.defaults.color = '#8b91a8';
Chart.defaults.borderColor = '#2a2f45';
Chart.defaults.font.family = "'DM Sans', sans-serif";

const C = { cyan:'#00e5c8', blue:'#4d9bff', amber:'#ffc246', red:'#ff5252', green:'#39e887', purple:'#b47cff', dim:'#555a72', };

let charts = {};
let allResults = [];
let simRunning = false;
let currentSpotCount = 0;
let allScenarios = [];

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
    method:  'POST',
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({}));
    alert('Error: ' + (err.detail || JSON.stringify(err)));
    return;
  }
  setRunning(true);
}

async function stopSim() { await fetch('/api/stop', { method: 'POST' }); }

async function clearResults() {
  if (!confirm('Clear all run history?')) return;
  await fetch('/api/results', { method: 'DELETE' });
  allResults = [];
  const tbl = document.getElementById('resultsTable');
  if (tbl) tbl.innerHTML = '<div class="results-empty">No runs yet. Start a scenario.</div>';
  updateComparisonChart();
  updateDeliveryChart();
  updateAIBadge();
  aiSetOutput('<p class="ai-placeholder">Run scenarios, then click <strong>Analyse</strong> for expert insights.</p>');
  aiSetTag('', '');
}

function setRunning(running) {
  simRunning = running;
  ['runPresetBtn', 'runCustomBtn'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.disabled = running;
  });
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
  setText('progElapsed', d.elapsed_s ?? '—');
  const simSec = d.simulated_elapsed_s ?? 0;
  const simH = Math.floor(simSec / 3600);
  const simM = Math.floor((simSec % 3600) / 60);
  setText('progSimTime', `${simH}h ${String(simM).padStart(2,'0')}m`);
  setText('progGenerated', d.generated ?? '—');
  setText('progReceived', d.cloud_events ?? '—');
  const occ = d.occupancy || {};
  const edge = d.edge || {};
  setText('progOccupancy', occ.occupancy_pct != null ? occ.occupancy_pct + '%' : '—');
  setText('progFiltered', edge.filtered ?? '—');
  setText('progDropped', edge.link_stats?.dropped ?? '—');
  const es = d.edge || {};
  setText('progAnomalies', es.anomalies ?? '—');
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
  ['progElapsed','progGenerated','progReceived','progOccupancy','progFiltered','progDropped']
    .forEach(id => setText(id, '—'));
}

function showResult(r, animate) {
  setText('kpi_lat_mean', r.latency_mean_ms?.toFixed(1) ?? '—');
  setText('kpi_lat_p50', r.latency_p50_ms?.toFixed(1) ?? '—');
  setText('kpi_lat_p95', r.latency_p95_ms?.toFixed(1) ?? '—');
  setText('kpi_lat_p99', r.latency_p99_ms?.toFixed(1) ?? '—');
  setText('kpi_delivery', ((r.sensor_to_edge_delivery_ratio ?? 1) * 100).toFixed(1));
  const agg = r.aggregation_ratio ?? 1;
  setText('kpi_agg', agg > 0 ? (1 / agg).toFixed(1) : '—');
  setText('kpi_filtered', r.filtered_events ?? '—');
  setText('kpi_bytes_se', r.sensor_to_edge_bytes ? (r.sensor_to_edge_bytes / 1024).toFixed(1) : '—');
  setText('kpi_bytes_ec', r.edge_to_cloud_bytes ? (r.edge_to_cloud_bytes / 1024).toFixed(1) : '—');
  updateLatencyHistogram(r.latency_samples || []);
  updateMsgCountChart(r);
  updateBandwidthChart(r);
  if (animate) buildLotGrid(r.num_spots);
  const fmt = (v, unit = '', d = 1) => v != null ? `${(+v).toFixed(d)}${unit}` : '—';
  const fmtPct = v => v != null ? `${(v * 100).toFixed(2)}%` : '—';
  const fmtKB = v => v ? `${(v / 1024).toFixed(1)} KB` : '—';

  setText('mt_events_generated', r.events_generated ?? '—');
  setText('mt_s2e_msgs', r.sensor_to_edge_msgs ?? '—');
  setText('mt_filtered', r.filtered_events ?? '—');
  setText('mt_dup_deliveries', r.duplicate_deliveries ?? '—');
  setText('mt_sensor_dropped', r.sensor_link_dropped ?? '—');
  setText('mt_e2c_msgs', r.edge_to_cloud_msgs || r.cloud_only_msgs || '—');
  setText('mt_e2c_dropped', r.edge_to_cloud_dropped ?? '—');
  setText('mt_transport_total', r.transport_msgs_total ?? '—');
  setText('mt_retransmissions', r.retransmissions_total ?? '—');
  setText('mt_cloud_msgs', r.events_reflected_in_cloud ?? '—');

  setText('mt_lat_mean', fmt(r.latency_mean_ms, ' ms'));
  setText('mt_lat_min', fmt(r.latency_min_ms, ' ms'));
  setText('mt_lat_p50', fmt(r.latency_p50_ms, ' ms'));
  setText('mt_lat_p95', fmt(r.latency_p95_ms, ' ms'));
  setText('mt_lat_p99', fmt(r.latency_p99_ms, ' ms'));
  setText('mt_lat_max', fmt(r.latency_max_ms, ' ms'));
  setText('mt_warmup', r.warmup_excluded_samples ?? '—');

  setText('mt_s2e_dr', fmtPct(r.sensor_to_edge_delivery_ratio));
  setText('mt_e2c_dr', fmtPct(r.edge_to_cloud_delivery_ratio));
  setText('mt_e2e_dr', fmtPct(r.end_to_end_delivery_ratio));
  setText('mt_phys_dr', fmtPct(r.physical_delivery_ratio));
  setText('mt_cloud_reflect', fmtPct(r.cloud_reflection_ratio));

  setText('mt_agg_ratio', fmt(r.aggregation_ratio, '', 4));
  setText('mt_msg_reduction', fmtPct(r.message_reduction_ratio));
  setText('mt_events_per_msg', fmt(r.events_per_cloud_message, '', 2));
  setText('mt_valid_changes', r.valid_state_changes ?? '—');
  setText('mt_events_reflected', r.events_reflected_in_cloud ?? '—');

  setText('mt_anomalies', r.anomalies_detected ?? '—');
  setText('mt_anom_resolved', r.anomalies_resolved ?? '—');
  setText('mt_anom_active', r.active_anomalies ?? '—');
  setText('mt_anom_spots', r.anomaly_detected_spots ?? '—');
  setText('mt_quarantined', r.quarantined_spots_peak ?? '—');
  setText('mt_mode_switches', r.adaptive_mode_switches ?? '—');
  setText('mt_faults', r.fault_injected_count ?? '—');

  setText('mt_bytes_se', fmtKB(r.sensor_to_edge_bytes));
  setText('mt_bytes_ec', fmtKB(r.edge_to_cloud_bytes));
  setText('mt_proto_bytes', fmtKB(r.protocol_bytes));
  setText('mt_broker_score', fmt(r.broker_overhead_score, '', 3));

  setText('mt_edge_mem', fmt(r.edge_mem_mb, ' MB', 1));
  setText('mt_cloud_mem', fmt(r.cloud_mem_mb, ' MB', 1));
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
        y: { title: { display: true, text: 'Count',        color: C.dim, font: { size: 9 } } },
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
  const s2e = r.sensor_to_edge_msgs || r.cloud_only_msgs || 0;
  charts.msgCount.data.datasets[0].data = [
    s2e, r.edge_to_cloud_msgs || 0, r.filtered_events || 0,
    Math.round(s2e * (1 - (r.sensor_to_edge_delivery_ratio || 1))),
  ];
  charts.msgCount.update('none');
}

function updateBandwidthChart(r) {
  if (!charts.bandwidth) return;
  charts.bandwidth.data.datasets[0].data = [
    +((r.sensor_to_edge_bytes || 0) / 1024).toFixed(2),
    +((r.edge_to_cloud_bytes  || 0) / 1024).toFixed(2),
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
  const data = allResults.map(r => +((r.sensor_to_edge_delivery_ratio || 1) * 100).toFixed(1));
  const colors = data.map(v => v >= 98 ? C.green + '99' : v >= 90 ? C.amber + '99' : C.red + '99');
  charts.delivery.data.labels = labels;
  charts.delivery.data.datasets[0].data = data;
  charts.delivery.data.datasets[0].backgroundColor = colors;
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
  if (el) el.textContent = val ?? '—';
}

async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`fetchJSON ${url} → ${r.status}`);
  return r.json();
}