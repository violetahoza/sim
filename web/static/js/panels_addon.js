(function () {
  const GRID_DEFAULT = '270px 1fr 300px';
  let leftCollapsed = false;
  let rightCollapsed = false;

  function cols() {
    const l = leftCollapsed ? '28px' : '270px';
    const r = rightCollapsed ? '28px' : '300px';
    return `${l} 1fr ${r}`;
  }

  function applyGrid() {
    const g = document.querySelector('.main-grid');
    if (g) g.style.gridTemplateColumns = cols();
  }

  window.toggleLeftPanel = function () {
    leftCollapsed = !leftCollapsed;
    const panel = document.querySelector('.panel-config');
    const btn = document.getElementById('leftCollapseBtn');
    if (!panel) return;
    panel.classList.toggle('collapsed', leftCollapsed);
    if (btn) btn.textContent = leftCollapsed ? '>' : '<';
    applyGrid();
  };

  window.toggleRightPanel = function () {
    rightCollapsed = !rightCollapsed;
    const panel = document.querySelector('.panel-right');
    const btn = document.getElementById('rightCollapseBtn');
    if (!panel) return;
    panel.classList.toggle('collapsed', rightCollapsed);
    if (btn) btn.textContent = rightCollapsed ? '<' : '>';
    applyGrid();
  };
})();


(function () {
  let _lastAIHtml = '';
  let _lastWarning = '';

  window.openAIModal = function () {
    const overlay = document.getElementById('aiModalOverlay');
    if (!overlay) return;

    const focusSel = document.getElementById('aiFocus');
    const scopeSel = document.getElementById('aiScope');
    const mFocus = document.getElementById('aiModalFocus');
    const mScope = document.getElementById('aiModalScope');
    const mTag = document.getElementById('aiModalTag');

    if (focusSel && mFocus) mFocus.value = focusSel.value;
    if (scopeSel && mScope) mScope.value = scopeSel.value;

    const sidebarOutput = document.getElementById('aiOutput');
    const modalBody = document.getElementById('aiModalBody');
    const sidebarTag = document.getElementById('aiModelTag');
    if (sidebarOutput && modalBody) modalBody.innerHTML = sidebarOutput.innerHTML;
    if (sidebarTag && mTag) {
      mTag.textContent = sidebarTag.textContent;
      mTag.className = sidebarTag.className.replace('ai-model-tag', 'ai-modal-tag');
    }

    overlay.classList.add('open');
  };

  window.closeAIModal = function () {
    const overlay = document.getElementById('aiModalOverlay');
    if (overlay) overlay.classList.remove('open');
  };

  document.addEventListener('click', function (e) {
    const overlay = document.getElementById('aiModalOverlay');
    if (overlay && e.target === overlay) closeAIModal();
  });

  window.runAIAnalysisModal = async function () {
    if (!allResults.length) {
      _setModalOutput('<p class="ai-error">⚠️ No results yet — run at least one scenario first.</p>');
      return;
    }

    const focus = document.getElementById('aiModalFocus')?.value ?? 'general';
    const scope = document.getElementById('aiModalScope')?.value ?? 'all';
    const btn = document.getElementById('aiModalAnalyseBtn');

    const toSend = scope === 'this' ? [currentResult ?? allResults[allResults.length - 1]]
      : scope === 'last3' ? allResults.slice(-3)
      : scope === 'last1' ? allResults.slice(-1)
      : allResults.slice();
    const stripped = toSend.map(r => { const c = {...r}; delete c.latency_samples; return c; });

    if (btn) { btn.disabled = true; btn.textContent = '⏳ Thinking…'; }
    const thinkingLabel = stripped.length === 1 ? `Analysing “${stripped[0].scenario_name || 'this run'}”…` : `Analysing ${stripped.length} runs…`;
    _setModalOutput(`<div class="ai-thinking"><span class="ai-spinner"></span> ${thinkingLabel}</div>`);

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

      _setModalOutput(html);

      const sidebarOut = document.getElementById('aiOutput');
      if (sidebarOut) sidebarOut.innerHTML = html;

      const tagText = data.powered_by || data.model || '';
      const tagCls = data.model === 'rule-based' ? 'tag-builtin' : 'tag-groq';
      const mTag = document.getElementById('aiModalTag');
      const sTag = document.getElementById('aiModelTag');
      if (mTag) { mTag.textContent = tagText; mTag.className = `ai-modal-tag ${tagCls}`; }
      if (sTag) { sTag.textContent = tagText; sTag.className = `ai-model-tag ${tagCls}`; }

      const mFocus = document.getElementById('aiModalFocus');
      const mScope = document.getElementById('aiModalScope');
      const sFocus = document.getElementById('aiFocus');
      const sScope = document.getElementById('aiScope');
      if (mFocus && sFocus) sFocus.value = mFocus.value;
      if (mScope && sScope) sScope.value = mScope.value;

    } catch (err) {
      console.error('[AI-modal]', err);
      _setModalOutput(`<div class="ai-error">⚠️ ${err.message}</div>`);
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = '✦ Analyse'; }
    }
  };

  function _setModalOutput(html) {
    const el = document.getElementById('aiModalBody');
    if (el) el.innerHTML = html;
  }
})();