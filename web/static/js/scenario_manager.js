let _editingScenario = null;
let _allScenariosCache = [];
let _compareSelected = new Set();

function _getResults() {
  if (typeof allResults !== 'undefined' && Array.isArray(allResults)) {
    return allResults;
  }
  return [];
}

function openMgr() {
  document.getElementById('mgrOverlay').classList.add('open');
  switchMgrTab('list');
  loadMgrList();
}

function closeMgr() {
  document.getElementById('mgrOverlay').classList.remove('open');
  _editingScenario = null;
}

function switchMgrTab(name) {
  ['list', 'create', 'edit', 'compare'].forEach(t => {
    document.getElementById(`mgrTab${cap(t)}`)?.classList.remove('active');
    document.getElementById(`mgrPane${cap(t)}`)?.classList.add('hidden');
  });
  document.getElementById(`mgrTab${cap(name)}`)?.classList.add('active');
  document.getElementById(`mgrPane${cap(name)}`)?.classList.remove('hidden');

  const saveBtn = document.getElementById('mgrSaveBtn');
  if (saveBtn) saveBtn.style.display = (name === 'create' || name === 'edit') ? 'inline-block' : 'none';

  if (name === 'compare') renderCompareTab();
}

function cap(s) { return s.charAt(0).toUpperCase() + s.slice(1); }

async function loadMgrList() {
  const list = document.getElementById('mgrList');
  if (!list) return;
  list.innerHTML = '<div style="color:var(--dim);font-size:.72rem;padding:.5rem 0">Loading…</div>';
  try {
    _allScenariosCache = await fetchJSON('/api/scenarios');
    list.innerHTML = '';
    const groups = {};
    _allScenariosCache.forEach(s => { const g = s.group || 'Ungrouped'; (groups[g] = groups[g] || []).push(s); });
    Object.entries(groups).forEach(([grpName, items]) => {
      const hdr = document.createElement('div');
      hdr.style.cssText = 'font-family:var(--mono);font-size:.6rem;color:var(--text3);text-transform:uppercase;letter-spacing:.1em;margin:.75rem 0 .3rem;padding-bottom:.25rem;border-bottom:1px solid var(--border)';
      hdr.textContent = grpName;
      list.appendChild(hdr);
      items.sort((a, b) => a.group_order - b.group_order).forEach(s => {
        const row = document.createElement('div');
        row.className = 'mgr-row';
        const dh = (s.sim_duration_s / 3600).toFixed(1);
        row.innerHTML = `
          <div class="mgr-row-info">
            <div class="mgr-row-name">${s.name}</div>
            <div class="mgr-row-meta">${s.protocol.toUpperCase()} · ${s.architecture} · ${s.traffic_level} · ${s.num_spots} spots · ${dh} h</div>
          </div>
          ${s.is_builtin ? '<span class="builtin-badge">built-in</span>' : ''}
          <div class="mgr-row-actions">
            <button class="mgr-btn edit" onclick="startEdit('${s.name}')">✎ Edit</button>
            ${!s.is_builtin ? `<button class="mgr-btn danger" onclick="deleteScenario('${s.name}')">✕ Delete</button>` : ''}
          </div>`;
        list.appendChild(row);
      });
    });
  } catch (e) {
    list.innerHTML = `<div style="color:var(--red);font-size:.72rem">Error: ${e.message}</div>`;
  }
}

function renderCompareTab() {
  const pane = document.getElementById('mgrPaneCompare');
  if (!pane) { console.warn('[Compare] mgrPaneCompare not found'); return; }

  const results = _getResults();

  if (results.length === 0) {
    pane.innerHTML = `
      <div class="cmp-empty">
        <div class="cmp-empty-icon">📊</div>
        <div class="cmp-empty-title">No runs yet</div>
        <div class="cmp-empty-sub">Run some scenarios from the dashboard first, then come back here to compare them.</div>
      </div>`;
    return;
  }

  pane.innerHTML = `
    <div class="cmp-layout">
      <div class="cmp-sidebar">
        <div class="cmp-sidebar-header">
          <span class="cmp-sidebar-title">SELECT RUNS</span>
          <div class="cmp-sidebar-actions">
            <button class="cmp-pill-btn" onclick="_cmpSelectAll()">All</button>
            <button class="cmp-pill-btn" onclick="_cmpSelectNone()">None</button>
          </div>
        </div>
        <div class="cmp-run-list" id="cmpRunList"></div>
      </div>
      <div class="cmp-main" id="cmpMain">
        <div class="cmp-placeholder">← Select at least 2 runs to compare</div>
      </div>
    </div>`;

  _compareSelected.clear();
  const autoSel = results.slice(-Math.min(3, results.length));
  autoSel.forEach((r, i) => {
    const globalIdx = results.length - autoSel.length + i;
    _compareSelected.add(r.scenario_name + '_' + globalIdx);
  });

  _renderCmpRunList(results);
  _renderCmpMain(results);
}

function _renderCmpRunList(results) {
  const list = document.getElementById('cmpRunList');
  if (!list) return;
  list.innerHTML = '';
  results.forEach((r, idx) => {
    const key = r.scenario_name + '_' + idx;
    const isSelected = _compareSelected.has(key);
    const protoColor = r.protocol === 'mqtt' ? 'var(--amber)' : r.protocol === 'coap' ? 'var(--purple)' : 'var(--green)';
    const item = document.createElement('div');
    item.className = 'cmp-run-item' + (isSelected ? ' selected' : '');
    item.dataset.idx = idx;
    item.innerHTML = `
      <div class="cmp-run-check">${isSelected ? '✓' : ''}</div>
      <div class="cmp-run-info">
        <div class="cmp-run-name" title="${r.scenario_name}">${_truncate(r.scenario_name, 26)}</div>
        <div class="cmp-run-meta">
          <span style="color:${protoColor}">${(r.protocol || '').toUpperCase()}</span>
          <span>·</span><span>${r.architecture || ''}</span>
          <span>·</span><span>${r.latency_mean_ms?.toFixed(1) ?? '—'}ms</span>
        </div>
      </div>`;
    item.onclick = () => _cmpToggle(key, item, results);
    list.appendChild(item);
  });
}

function _cmpToggle(key, itemEl, results) {
  if (_compareSelected.has(key)) {
    _compareSelected.delete(key);
    itemEl.classList.remove('selected');
    itemEl.querySelector('.cmp-run-check').textContent = '';
  } else {
    _compareSelected.add(key);
    itemEl.classList.add('selected');
    itemEl.querySelector('.cmp-run-check').textContent = '✓';
  }
  _renderCmpMain(results);
}

function _cmpSelectAll() {
  const results = _getResults();
  results.forEach((r, i) => _compareSelected.add(r.scenario_name + '_' + i));
  _renderCmpRunList(results);
  _renderCmpMain(results);
}

function _cmpSelectNone() {
  _compareSelected.clear();
  const results = _getResults();
  _renderCmpRunList(results);
  _renderCmpMain(results);
}

function _getSelectedResults(results) {
  return results.filter((r, i) => _compareSelected.has(r.scenario_name + '_' + i));
}

function _renderCmpMain(results) {
  const main = document.getElementById('cmpMain');
  if (!main) return;
  const selected = _getSelectedResults(results);

  if (selected.length < 1) {
    main.innerHTML = `<div class="cmp-placeholder">← Select at least 1 run to view metrics</div>`;
    return;
  }

  ['cmpLatChart', 'cmpDelChart', 'cmpMsgChart', 'cmpBwChart'].forEach(id => {
    const existing = Chart.getChart(id);
    if (existing) existing.destroy();
  });

  main.innerHTML = `
    <div class="cmp-results">
      <div class="cmp-kpi-strip" id="cmpKpiStrip"></div>
      <div class="cmp-charts-wrap">
        <div class="cmp-chart-box">
          <div class="cmp-chart-title">LATENCY PROFILE (ms)</div>
          <canvas id="cmpLatChart" style="max-height:160px"></canvas>
        </div>
        <div class="cmp-chart-box">
          <div class="cmp-chart-title">REAL EVENTS CAPTURED (%)</div>
          <canvas id="cmpDelChart" style="max-height:160px"></canvas>
        </div>
        <div class="cmp-chart-box">
          <div class="cmp-chart-title">MESSAGE COUNTS</div>
          <canvas id="cmpMsgChart" style="max-height:160px"></canvas>
        </div>
        <div class="cmp-chart-box">
          <div class="cmp-chart-title">BANDWIDTH (KB)</div>
          <canvas id="cmpBwChart" style="max-height:160px"></canvas>
        </div>
      </div>
      <div class="cmp-table-wrap">
        <div class="cmp-chart-title" style="margin-bottom:.6rem">FULL METRICS TABLE</div>
        <div id="cmpTable"></div>
      </div>
    </div>`;

  _buildCmpKpis(selected);
  _buildCmpCharts(selected);
  _buildCmpTable(selected);
}

const _isCloudOnly = r => r.architecture === 'cloud_only';
const _isAggregated = r => r.architecture === 'edge_aggregated';
const _isEdge = r => !_isCloudOnly(r);  

const _truncate = (s, n) => s && s.length > n ? s.slice(0, n - 1) + '…' : (s || '—');

const _int = v => v == null ? '—' : String(v);
const _ms = v => v == null ? '—' : v.toFixed(1) + ' ms';
const _pct = v => v == null ? '—' : (v * 100).toFixed(2) + '%';
const _pct1 = v => v == null ? '—' : (v * 100).toFixed(1) + '%';
const _kb = v => v == null ? '—' : (v / 1024).toFixed(1);
const _mb = v => v == null ? '—' : v.toFixed(1);
const _f2 = v => v == null ? '—' : v.toFixed(2);
const _f3 = v => v == null ? '—' : v.toFixed(3);
const _dur = v => v == null ? '—' : `${(v / 3600).toFixed(1)} h`;
const _str = v => v == null ? '—' : String(v);


function _pickBy(arr, score, mode = 'min') {
  const cands = arr.filter(r => score(r) != null);
  if (!cands.length) return null;
  return cands.reduce((a, b) => {
    const cmp = mode === 'min' ? score(a) < score(b) : score(a) > score(b);
    return cmp ? a : b;
  });
}

function _buildCmpKpis(sel) {
  const strip = document.getElementById('cmpKpiStrip');
  if (!strip) return;

  const bestLat = _pickBy(sel, r => r.latency_mean_ms, 'min');
  const bestCoverage = _pickBy(sel, r => r.cloud_reflection_ratio, 'max');
  const bestSavings = _pickBy(sel.filter(_isEdge), r => r.message_reduction_ratio, 'max');
  const lowestCloudBw = _pickBy(sel, r => r.bytes_e2c_sent ?? r.edge_to_cloud_bytes, 'min');
  const fewestRetries = sel.length > 1 ? _pickBy(sel, r => r.proto_retransmissions ?? r.retransmissions_total ?? 0, 'min') : null;

  const cards = [
    bestLat && { label: 'FASTEST', icon: '⚡',  name: bestLat.scenario_name, val: `${bestLat.latency_mean_ms?.toFixed(1)}ms mean`, color: 'var(--cyan)' },
    bestCoverage && { label: 'MOST EVENTS CAPTURED', icon: '🎯', name: bestCoverage.scenario_name, val: `${(bestCoverage.cloud_reflection_ratio * 100).toFixed(1)}% of real events`, color: 'var(--green)' },
    bestSavings && { label: 'LEAST CLOUD TRAFFIC', icon: '💾', name: bestSavings.scenario_name, val: `${(bestSavings.message_reduction_ratio * 100).toFixed(1)}% saved`, color: 'var(--blue)' },
    lowestCloudBw && { label: 'LOWEST BANDWIDTH', icon: '📡', name: lowestCloudBw.scenario_name, val: `${((lowestCloudBw.bytes_e2c_sent ?? lowestCloudBw.edge_to_cloud_bytes ?? 0) / 1024).toFixed(1)} KB`, color: 'var(--purple)' },
    fewestRetries && { label: 'FEWEST RETRIES', icon: '🔁', name: fewestRetries.scenario_name, val: `${fewestRetries.proto_retransmissions ?? fewestRetries.retransmissions_total ?? 0} retries`, color: 'var(--green)' },
  ].filter(Boolean);

  strip.innerHTML = cards.map(k => `
    <div class="cmp-kpi-card" style="--kpi-color:${k.color}">
      <div class="cmp-kpi-icon">${k.icon}</div>
      <div class="cmp-kpi-label">${k.label}</div>
      <div class="cmp-kpi-winner" title="${k.name}">${_truncate(k.name, 20)}</div>
      <div class="cmp-kpi-val">${k.val}</div>
    </div>`).join('');
}

const _CMP_PAL = ['#00e5c8','#4d9bff','#ffc246','#b47cff','#39e887','#ff5252','#ff9040','#00b4d8'];

function _makeChart(id, type, data, options) {
  const el = document.getElementById(id);
  if (!el) return null;
  return new Chart(el, { type, data, options });
}

function _buildCmpCharts(sel) {
  const labels = sel.map(r => _truncate(r.scenario_name, 16));
  const pal = _CMP_PAL;
  const base = { responsive: true, maintainAspectRatio: false };
  const anyEdge = sel.some(_isEdge);

  _makeChart('cmpLatChart', 'bar', {
    labels,
    datasets: [
      { label: 'Mean', data: sel.map(r => r.latency_mean_ms ?? 0), backgroundColor: pal[0]+'bb', borderColor: pal[0], borderWidth: 1, borderRadius: 4 },
      { label: 'P95', data: sel.map(r => r.latency_p95_ms  ?? 0), backgroundColor: pal[1]+'bb', borderColor: pal[1], borderWidth: 1, borderRadius: 4 },
      { label: 'P99', data: sel.map(r => r.latency_p99_ms  ?? 0), backgroundColor: pal[2]+'bb', borderColor: pal[2], borderWidth: 1, borderRadius: 4 },
    ],
  }, { ...base,
    plugins: { legend: { position: 'bottom', labels: { font: { size: 9 }, padding: 6 } } },
    scales:  { y: { beginAtZero: true, title: { display: true, text: 'ms', color: '#555a72', font: { size: 9 } } } } });

  _makeChart('cmpDelChart', 'bar', {
    labels,
    datasets: [{
      label: 'Real events captured %',
      data: sel.map(r => +((r.cloud_reflection_ratio ?? 1) * 100).toFixed(2)),
      backgroundColor: sel.map(r => { const v = (r.cloud_reflection_ratio ?? 1) * 100; return v >= 98 ? '#39e887bb' : v >= 90 ? '#ffc246bb' : '#ff5252bb'; }),
      borderColor: sel.map(r => { const v = (r.cloud_reflection_ratio ?? 1) * 100; return v >= 98 ? '#39e887' : v >= 90 ? '#ffc246' : '#ff5252'; }),
      borderWidth: 1, borderRadius: 4,
    }],
  }, { ...base,
    plugins: { legend: { display: false } },
    scales:  { y: { min: 0, max: 100, title: { display: true, text: '%', color: '#555a72', font: { size: 9 } } } } });

  const msgDatasets = [
    { label: 'Generated', data: sel.map(r => r.events_generated_total ?? r.events_generated ?? 0), backgroundColor: pal[3]+'66', borderColor: pal[3], borderWidth: 1, borderRadius: 4 },
    { label: 'Sensor Sent', data: sel.map(r => r.frames_s2e_sent ?? r.sensor_to_edge_msgs ?? 0), backgroundColor: pal[1]+'bb', borderColor: pal[1], borderWidth: 1, borderRadius: 4 },
    { label: 'Reached Cloud', data: sel.map(r => r.cloud_events_pre_dedup ?? r.cloud_msgs_received ?? r.cloud_msgs_received_total ?? 0), backgroundColor: pal[0]+'bb', borderColor: pal[0], borderWidth: 1, borderRadius: 4 },
  ];
  if (anyEdge) {
    msgDatasets.push({
      label: 'Filtered (edge)',
      data: sel.map(r => _isCloudOnly(r) ? 0 : (r.events_filtered_total ?? r.filtered_events ?? 0)),
      backgroundColor: pal[2]+'88', borderColor: pal[2], borderWidth: 1, borderRadius: 4,
    });
  }
  _makeChart('cmpMsgChart', 'bar', { labels, datasets: msgDatasets }, {
    ...base,
    plugins: { legend: { position: 'bottom', labels: { font: { size: 9 }, padding: 6 } } },
    scales: { y: { beginAtZero: true } },
  });

  const onlyCloud = !anyEdge;
  _makeChart('cmpBwChart', 'bar', {
    labels,
    datasets: [
      { label: onlyCloud ? 'Sensor → Cloud (KB)' : 'Sensor → Edge (KB)',
        data: sel.map(r => +(((r.bytes_s2e_sent ?? r.sensor_to_edge_bytes ?? 0)) / 1024).toFixed(2)),
        backgroundColor: pal[1]+'bb', borderColor: pal[1], borderWidth: 1, borderRadius: 4 },
      { label: onlyCloud ? 'Reached Cloud (KB)' : 'Edge → Cloud (KB)',
        data: sel.map(r => +(((r.bytes_e2c_sent ?? r.edge_to_cloud_bytes ?? 0)) / 1024).toFixed(2)),
        backgroundColor: pal[0]+'bb', borderColor: pal[0], borderWidth: 1, borderRadius: 4 },
    ],
  }, { ...base,
    plugins: { legend: { position: 'bottom', labels: { font: { size: 9 }, padding: 6 } } },
    scales:  { y: { beginAtZero: true, title: { display: true, text: 'KB', color: '#555a72', font: { size: 9 } } } } });
}

const _always = () => true;

function _metricsList() {
  const G = label => ({ group: label });
  return [
    G('Configuration'),
    { label: 'Protocol', value: r => (r.protocol || '').toUpperCase(), applies: _always, fmt: _str },
    { label: 'Architecture', value: r => r.architecture || null, applies: _always, fmt: _str },
    { label: 'Traffic', value: r => r.traffic_level || null, applies: _always, fmt: _str },
    { label: 'Spots', value: r => r.num_spots ?? null, applies: _always, fmt: _int },
    { label: 'Duration', value: r => r.sim_duration_s ?? null, applies: _always, fmt: _dur },

    G('Sensor Activity'),
    { label: 'Sensor messages emitted', value: r => r.events_generated_total ?? r.events_generated ?? null, applies: _always, fmt: _int },
    { label: 'Real arrivals & departures', value: r => r.state_changes_generated_total ?? r.valid_state_changes ?? null, applies: _always, fmt: _int },
    { label: 'Startup occupancy snapshots', value: r => r.initial_snapshots_generated_total ?? r.initial_snapshots_generated ?? null, applies: _always, fmt: _int },
    { label: 'Duplicate retransmits', value: r => r.duplicate_sends_generated_total ?? r.duplicate_sends_generated ?? null, applies: _always, fmt: _int },
    { label: 'Heartbeats emitted', value: r => r.heartbeats_generated_total ?? r.heartbeats_generated ?? null, applies: _always, fmt: _int },
    { label: 'Heartbeat interval (s)', value: r => r.heartbeat_interval_s ?? null, applies: _always, fmt: _f2 },

    G('Wireless link (sensor → first hop)'),
    { label: 'Unique messages sent', value: r => r.frames_s2e_sent ?? r.sensor_to_edge_msgs ?? null, applies: _always, fmt: _int },
    { label: 'Lost on the radio', value: r => r.frames_s2e_dropped ?? r.sensor_link_dropped ?? null, applies: _always, fmt: _int, better: 'low'  },
    { label: 'Wireless delivery', value: r => r.s2e_delivery_ratio ?? r.sensor_to_edge_delivery_ratio ?? null,  applies: _always, fmt: _pct, better: 'high' },

    G('Edge processing'),
    { label: 'Filtered as redundant', value: r => r.events_filtered_total ?? r.filtered_events ?? null, applies: _isEdge, fmt: _int,  better: 'high' },
    { label: 'Forwarded to broker', value: r => r.frames_e2c_sent ?? r.edge_to_cloud_msgs ?? null, applies: _isEdge, fmt: _int,  better: 'low'  },
    { label: 'Cloud traffic saved', value: r => r.message_reduction_ratio ?? null, applies: _isEdge, fmt: _pct1, better: 'high' },
    { label: 'Avg events per batch', value: r => r.events_per_cloud_message ?? null, applies: _isAggregated, fmt: _f2, better: 'high' },

    G('Backhaul link (edge → broker)'),
    { label: 'Backhaul delivery', value: r => r.backhaul_delivery_ratio ?? r.edge_to_cloud_delivery_ratio ?? null, applies: _isEdge, fmt: _pct, better: 'high' },
    { label: 'Lost on backhaul', value: r => r.frames_e2c_dropped ?? r.edge_to_cloud_dropped ?? null, applies: _isEdge, fmt: _int, better: 'low'  },

    G('Broker layer (MQTT/AMQP/CoAP)'),
    { label: 'Broker retries (QoS)', value: r => r.proto_retransmissions ?? r.retransmissions_total ?? null, applies: _always, fmt: _int, better: 'low' },
    { label: 'Duplicates caught by protocol', value: r => r.proto_duplicate_deliveries ?? r.duplicate_deliveries ?? null, applies: _always, fmt: _int },

    G('Cloud State'),
    { label: 'Total msgs received', value: r => r.cloud_events_pre_dedup ?? r.cloud_msgs_received ?? r.cloud_msgs_received_total ?? null, applies: _always, fmt: _int },
    { label: 'State transitions reflected', value: r => r.unique_state_changes_applied_at_cloud ?? r.cloud_state_changes_reflected ?? null, applies: _isEdge, fmt: _int },
    { label: 'Real events captured', value: r => r.cloud_reflection_ratio ?? null, applies: _always, fmt: _pct, better: 'high' },
    
    G('Latency (sensor emits → cloud stores)'),
    { label: 'Mean', value: r => r.latency_mean_ms ?? null, applies: _always, fmt: _ms, better: 'low' },
    { label: 'P50', value: r => r.latency_p50_ms ?? null, applies: _always, fmt: _ms, better: 'low' },
    { label: 'P95', value: r => r.latency_p95_ms ?? null, applies: _always, fmt: _ms, better: 'low' },
    { label: 'P99', value: r => r.latency_p99_ms ?? null, applies: _always, fmt: _ms, better: 'low' },
    { label: 'Min', value: r => r.latency_min_ms ?? null, applies: _always, fmt: _ms, better: 'low' },
    { label: 'Max', value: r => r.latency_max_ms ?? null, applies: _always, fmt: _ms, better: 'low' },

    G('Anomaly Detection'),
    { label: 'Detected', value: r => r.anomalies_detected ?? null, applies: _isEdge, fmt: _int, better: 'low' },
    { label: 'Resolved', value: r => r.anomalies_resolved ?? null, applies: _isEdge, fmt: _int },
    { label: 'Active at end', value: r => r.active_anomalies ?? null, applies: _isEdge, fmt: _int, better: 'low' },
    { label: 'Affected spots', value: r => r.anomaly_detected_spots ?? null, applies: _isEdge, fmt: _int, better: 'low' },
    { label: 'Quarantined at end', value: r => r.quarantined_spots_final ?? null, applies: _isEdge, fmt: _int, better: 'low' },
    { label: 'Mode switches', value: r => r.adaptive_mode_switches ?? null, applies: _isEdge, fmt: _int },
    { label: 'Faults injected', value: r => r.fault_injected_count ?? null, applies: r => _isEdge(r) && (r.fault_injected_count ?? 0) > 0, fmt: _int },

    G('Bandwidth'),
    { label: 'Sensor → first hop (KB)', value: r => r.bytes_s2e_sent ?? r.sensor_to_edge_bytes ?? null, applies: _always, fmt: _kb, better: 'low' },
    { label: 'To cloud (KB)', value: r => r.bytes_e2c_sent ?? r.edge_to_cloud_bytes ?? null, applies: _always, fmt: _kb, better: 'low' },
    { label: 'Protocol overhead (KB)', value: r => r.proto_bytes_sent ?? r.protocol_bytes ?? null, applies: _always, fmt: _kb, better: 'low' },
  ];
}

function _buildCmpTable(sel) {
  const wrap = document.getElementById('cmpTable');
  if (!wrap) return;
  const visibleList = _filterEmptyGroups(_metricsList(), sel);
  let html = '<div class="cmp-table">';

  html += '<div class="cmp-table-row cmp-table-head"><div class="cmp-table-cell cmp-metric-col">METRIC</div>';
  sel.forEach(r => {
    const c = r.protocol === 'mqtt' ? 'var(--amber)' : r.protocol === 'coap' ? 'var(--purple)' : 'var(--green)';
    html += `<div class="cmp-table-cell" style="color:${c}" title="${r.scenario_name}">${_truncate(r.scenario_name, 20)}</div>`;
  });
  html += '</div>';

  visibleList.forEach(m => {
    if (m.group) {
      html += `<div class="cmp-table-row cmp-group-row"><div class="cmp-table-cell cmp-group-header">${m.group}</div></div>`;
      return;
    }
    html += _renderMetricRow(m, sel);
  });

  html += '</div>';
  wrap.innerHTML = html;
}


function _filterEmptyGroups(list, sel) {
  const out = [];
  let pendingGroup = null;

  list.forEach(m => {
    if (m.group) {
      pendingGroup = m;
      return;
    }
    const anyAnswers = sel.some(r => m.applies(r));
    if (!anyAnswers) return;
    if (pendingGroup) {
      out.push(pendingGroup);
      pendingGroup = null;
    }
    out.push(m);
  });
  return out;
}

function _renderMetricRow(m, sel) {
  const values = sel.map(r => m.applies(r) ? m.value(r) : null);

  let bestVal = null, worstVal = null;
  if (m.better) {
    const nums = values.filter(v => typeof v === 'number' && !isNaN(v));
    if (nums.length > 1) {
      if (m.better === 'low')  { bestVal = Math.min(...nums); worstVal = Math.max(...nums); }
      if (m.better === 'high') { bestVal = Math.max(...nums); worstVal = Math.min(...nums); }
    }
  }

  let html = `<div class="cmp-table-row"><div class="cmp-table-cell cmp-metric-col">${m.label}</div>`;
  values.forEach(v => {
    const display = m.fmt(v);
    let cls = '';
    if (bestVal  !== null && v === bestVal)  cls = 'cmp-best';
    else if (worstVal !== null && v === worstVal) cls = 'cmp-worst';
    html += `<div class="cmp-table-cell ${cls}">${display}</div>`;
  });
  html += '</div>';
  return html;
}

function updateCreateProtoOpts() { _toggleProtoFields('cf', document.getElementById('cf_protocol')?.value); }
function updateEditProtoOpts() { _toggleProtoFields('ef', document.getElementById('ef_protocol')?.value); }

function _toggleProtoFields(prefix, proto) {
  [`${prefix}_amqp`,`${prefix}_amqp2`,`${prefix}_amqp3`,`${prefix}_mqtt`,`${prefix}_coap`]
    .forEach(id => { const el=document.getElementById(id); if(el) el.style.display='none'; });
  if (proto === 'amqp') {
    ['amqp','amqp2','amqp3'].forEach(s => { const el=document.getElementById(`${prefix}_${s}`); if(el) el.style.display=''; });
  } else {
    const el = document.getElementById(`${prefix}_${proto}`);
    if (el) el.style.display = '';
  }
}

function startEdit(name) {
  const s = _allScenariosCache.find(x => x.name === name);
  if (!s) return;
  _editingScenario = name;
  const set = (id, val) => { const el=document.getElementById(id); if(el) el.value=val??''; };
  set('ef_name', s.name);
  set('ef_desc', s.description);
  set('ef_group', s.group);
  set('ef_gorder', s.group_order);
  set('ef_protocol', s.protocol);
  set('ef_arch', s.architecture);
  set('ef_traffic', s.traffic_level);
  set('ef_spots', s.num_spots);
  set('ef_duration', (s.sim_duration_s / 3600).toFixed(1));
  set('ef_seed', s.seed ?? 42);
  set('ef_loss', ((s.loss_rate ?? 0.02) * 100).toFixed(1));
  set('ef_ratelimit', s.rate_limit ?? 5);
  set('ef_agg', s.aggregation_interval ?? 30);
  set('ef_amqp_exchange', s.amqp_exchange ?? 'direct');
  set('ef_amqp_ack', s.amqp_ack ?? 'manual');
  set('ef_amqp_durable', (s.amqp_durable ?? true) ? 'true' : 'false');
  set('ef_qos', s.mqtt_qos ?? 1);
  set('ef_coap_mode', s.coap_mode ?? 'CON');
  updateEditProtoOpts();
  const isBuiltin = s.is_builtin;
  ['ef_protocol','ef_arch','ef_traffic','ef_spots','ef_duration','ef_seed','ef_loss','ef_ratelimit',
   'ef_agg','ef_amqp_exchange','ef_amqp_ack','ef_amqp_durable','ef_qos','ef_coap_mode'].forEach(id => {
    const el=document.getElementById(id); if(el){ el.disabled=isBuiltin; el.style.opacity=isBuiltin?'.45':'1'; }
  });
  const saveBtn = document.getElementById('mgrSaveBtn');
  if (saveBtn) saveBtn.style.display = isBuiltin ? 'none' : 'inline-block';
  document.getElementById('mgrTabEdit')?.classList.remove('hidden');
  switchMgrTab('edit');
}

async function deleteScenario(name) {
  if (!confirm(`Delete scenario "${name}"? This cannot be undone.`)) return;
  try {
    const r = await fetch(`/api/scenarios/${encodeURIComponent(name)}`, { method:'DELETE' });
    if (!r.ok) { const d=await r.json().catch(()=>{}); throw new Error(d?.detail??`${r.status}`); }
    showToast(`Deleted "${name}"`, 'var(--red)');
    await loadMgrList(); await loadScenarios();
  } catch(e) { showToast(`Error: ${e.message}`, 'var(--red)'); }
}

async function saveMgrForm() {
  const isEdit = !document.getElementById('mgrPaneEdit')?.classList.contains('hidden');
  const get = id => document.getElementById(id)?.value ?? '';
  const body = isEdit ? {
    description: get('ef_desc'),
    group: get('ef_group'),
    group_order: +get('ef_gorder') || 99,
    protocol: get('ef_protocol'),
    architecture: get('ef_arch'),
    traffic_level: get('ef_traffic'),
    num_spots: +get('ef_spots'),
    sim_duration_h: +get('ef_duration'),
    seed: +get('ef_seed'),
    loss_rate: +get('ef_loss') / 100,
    rate_limit: +get('ef_ratelimit'),
    aggregation_interval: +get('ef_agg'),
    amqp_exchange: get('ef_amqp_exchange'),
    amqp_ack: get('ef_amqp_ack'),
    amqp_durable: get('ef_amqp_durable') === 'true',
    mqtt_qos: +get('ef_qos'),
    coap_mode: get('ef_coap_mode')
  } : {
    name: get('cf_name').trim().replace(/\s+/g,'_'),
    description: get('cf_desc') || get('cf_name'),
    group: get('cf_group') || 'User Scenarios',
    group_order: +get('cf_gorder') || 99,
    protocol: get('cf_protocol'),
    architecture: get('cf_arch'),
    traffic_level: get('cf_traffic'),
    num_spots: +get('cf_spots'),
    sim_duration_h: +get('cf_duration'),
    seed: +get('cf_seed'),
    loss_rate: +get('cf_loss') / 100,
    rate_limit: +get('cf_ratelimit'),
    aggregation_interval: +get('cf_agg'),
    amqp_exchange: get('cf_amqp_exchange'),
    amqp_ack: get('cf_amqp_ack'),
    amqp_durable: get('cf_amqp_durable') === 'true',
    mqtt_qos: +get('cf_qos'),
    coap_mode: get('cf_coap_mode')
  };
  if (!isEdit && !body.name) { showToast('Name is required','var(--amber)'); return; }
  try {
    const url = isEdit ? `/api/scenarios/${encodeURIComponent(_editingScenario)}` : '/api/scenarios';
    const meth = isEdit ? 'PUT' : 'POST';
    const r = await fetch(url, { method:meth, headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) });
    if (!r.ok) { const d=await r.json().catch(()=>{}); throw new Error(d?.detail??`${r.status}`); }
    showToast(isEdit ? `Saved "${_editingScenario}"` : `Created "${body.name}"`, 'var(--cyan)');
    closeMgr(); await loadScenarios();
  } catch(e) { showToast(`Error: ${e.message}`, 'var(--red)'); }
}

async function saveCustomAsPreset() {
  const name = prompt('Scenario name (no spaces):');
  if (!name?.trim()) return;
  const cleanName = name.trim().replace(/\s+/g,'_');
  const get = id => document.getElementById(id)?.value ?? '';
  const body = {
    name: cleanName,
    description: `Custom: ${cleanName}`,
    group: 'User Scenarios',
    group_order: 99,
    protocol: get('c_protocol'),
    architecture: get('c_arch'),
    traffic_level: get('c_traffic'),
    num_spots: +get('c_spots'),
    sim_duration_h: +get('c_duration'),
    seed: 42,
    loss_rate: +get('c_loss') / 100,
    rate_limit: +get('c_ratelimit'),
    aggregation_interval: +get('c_agg'),
    amqp_exchange: get('c_amqp_exchange') || 'direct',
    amqp_ack: get('c_amqp_ack') || 'manual',
    amqp_durable: (get('c_amqp_durable') || 'true') === 'true',
    mqtt_qos: +(get('c_qos') || 1),
    coap_mode: get('c_coap_mode') || 'CON'
  };
  try {
    const r = await fetch('/api/scenarios',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if (!r.ok) { const d=await r.json().catch(()=>{}); throw new Error(d?.detail??`${r.status}`); }
    showToast(`Saved as preset "${cleanName}"`, 'var(--cyan)');
    await loadScenarios();
  } catch(e) { showToast(`Error: ${e.message}`, 'var(--red)'); }
}

function showToast(msg, color='var(--cyan)') {
  const t = document.createElement('div');
  t.className = 'toast'; t.style.borderColor = color; t.style.color = color; t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(() => t.remove(), 2800);
}