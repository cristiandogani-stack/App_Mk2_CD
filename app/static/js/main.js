// Track navigation between top-level modules and normalise the behaviour of
// "back" buttons throughout the app.  Each page stores the current module and
// URL in sessionStorage so that subsequent pages can determine where the user
// arrived from.  Back buttons with the `.back-link` class automatically use the
// previously visited module (tab) when possible, falling back to the element's
// href or `data-fallback-url` attribute.
document.addEventListener('DOMContentLoaded', () => {
  const body = document.body;
  const moduleSlug = body ? (body.getAttribute('data-module') || '') : '';
  const currentPath = window.location.pathname + window.location.search;
  const origin = window.location.origin;

  const toAbsolute = (value) => {
    if (!value) {
      return '';
    }
    try {
      if (value.startsWith('http://') || value.startsWith('https://')) {
        const url = new URL(value);
        return url.origin + url.pathname + url.search;
      }
      if (value.startsWith('//')) {
        const url = new URL(window.location.protocol + value);
        return url.origin + url.pathname + url.search;
      }
      const path = value.startsWith('/') ? value : `/${value}`;
      return origin + path;
    } catch (err) {
      return value;
    }
  };

  try {
    const lastModule = sessionStorage.getItem('currentModule');
    if (lastModule && lastModule !== moduleSlug) {
      sessionStorage.setItem('previousModule', lastModule);
    } else if (!lastModule) {
      sessionStorage.removeItem('previousModule');
    }
    if (moduleSlug) {
      sessionStorage.setItem('currentModule', moduleSlug);
      sessionStorage.setItem(`lastUrlFor:${moduleSlug}`, currentPath);
    } else {
      sessionStorage.removeItem('currentModule');
    }
  } catch (err) {
    // Ignore storage errors (private mode, disabled storage, etc.)
  }

  const resolveBackTarget = (moduleHint) => {
    const absoluteCurrent = origin + window.location.pathname + window.location.search;
    let target = '';
    try {
      const hint = moduleHint || sessionStorage.getItem('previousModule') || '';
      if (hint) {
        const stored = sessionStorage.getItem(`lastUrlFor:${hint}`) || '';
        if (stored) {
          const absStored = toAbsolute(stored);
          if (absStored && absStored !== absoluteCurrent) {
            target = stored.startsWith('http') ? absStored : (stored.startsWith('/') ? stored : `/${stored}`);
            return target;
          }
        }
      }
    } catch (err) {
      // Ignore storage errors
    }

    try {
      if (document.referrer) {
        const refUrl = new URL(document.referrer, origin);
        const refAbsolute = refUrl.origin + refUrl.pathname + refUrl.search;
        if (refAbsolute !== absoluteCurrent) {
          if (refUrl.origin === origin) {
            return refUrl.pathname + refUrl.search;
          }
          return document.referrer;
        }
      }
    } catch (err) {
      // If referrer parsing fails, fall through
      if (document.referrer) {
        return document.referrer;
      }
    }

    return target;
  };

  document.addEventListener('click', (event) => {
    const backEl = event.target.closest('a.back-link, button.back-link');
    if (!backEl) {
      return;
    }
    const behaviour = backEl.dataset.moduleBack || '';
    if (behaviour === 'off') {
      return;
    }
    const href = backEl.getAttribute('href');
    if (behaviour === 'fallback') {
      let finalTarget = backEl.dataset.fallbackUrl || '';
      if (!finalTarget && href && href !== '#') {
        finalTarget = href;
      }
      if (!finalTarget) {
        return;
      }
      event.preventDefault();
      if (finalTarget.startsWith('http://') || finalTarget.startsWith('https://')) {
        window.location.href = finalTarget;
      } else if (finalTarget.startsWith('//')) {
        window.location.href = window.location.protocol + finalTarget;
      } else {
        const path = finalTarget.startsWith('/') ? finalTarget : `/${finalTarget}`;
        window.location.href = path;
      }
      return;
    }
    let moduleHint = backEl.dataset.backModule || '';
    if (moduleHint === 'current') {
      moduleHint = moduleSlug;
    }
    const destination = resolveBackTarget(moduleHint);
    let finalTarget = destination;
    if (!finalTarget && backEl.dataset.fallbackUrl) {
      finalTarget = backEl.dataset.fallbackUrl;
    }
    if (!finalTarget && href && href !== '#') {
      finalTarget = href;
    }
    if (!finalTarget) {
      return;
    }
    event.preventDefault();
    if (finalTarget.startsWith('http://') || finalTarget.startsWith('https://')) {
      window.location.href = finalTarget;
    } else if (finalTarget.startsWith('//')) {
      window.location.href = window.location.protocol + finalTarget;
    } else {
      const path = finalTarget.startsWith('/') ? finalTarget : `/${finalTarget}`;
      window.location.href = path;
    }
  }, { capture: true });
});

// Mobile menu toggle & theme cycling
document.addEventListener('DOMContentLoaded', () => {
  const burger = document.getElementById('hamburger');
  const menu = document.getElementById('mobileMenu');
  if (burger && menu){
    burger.addEventListener('click', () => menu.classList.toggle('show'));
  }
  const toggle = document.getElementById('themeToggle');
  if (toggle){
    const applyTheme = (t) => {
      // Apply a `data-theme` attribute on the <html> tag so that CSS rules can react
      document.documentElement.setAttribute('data-theme', t);
      // Persist the choice so that reloads remember the preference
      try{ localStorage.setItem('theme', t); }catch(e){}
      // Update the <meta name="color-scheme"> tag to hint the browser about our current choice.
      // Without this the browser may render form controls (like inputs) in the wrong style.
      const meta = document.querySelector('meta[name="color-scheme"]');
      if (meta){
        if (t === 'dark') meta.content = 'dark light';
        else if (t === 'light') meta.content = 'light dark';
        else meta.content = 'dark light';
      }
    };
    let current = localStorage.getItem('theme') || 'system';
    applyTheme(current);
    toggle.addEventListener('click', () => {
      current = current === 'light' ? 'dark' : (current === 'dark' ? 'system' : 'light');
      applyTheme(current);
    });
  }
});

// Global search functionality for component lookup.  This logic is separated
// from the theme toggle so that both can run independently on DOMContentLoaded.
document.addEventListener('DOMContentLoaded', function(){
  const searchForm = document.getElementById('global-search-form');
  const searchInput = document.getElementById('global-search-input');
  const datalist = document.getElementById('global-search-list');
  // Map from suggestion label to its associated metadata (product_id,
  // component_id, category).  This is populated whenever suggestions are
  // fetched from the server.
  let suggestionMap = {};
  if (searchInput && datalist && searchForm) {
    searchInput.addEventListener('input', function(){
      const query = searchInput.value.trim();
      // Only fetch suggestions for queries with at least 2 characters
      if (query.length < 2) {
        datalist.innerHTML = '';
        suggestionMap = {};
        return;
      }
      // Fetch suggestions from the server and populate the datalist
      fetch(`/products/search_suggestions?q=${encodeURIComponent(query)}`)
        .then(resp => resp.json())
        .then(data => {
          suggestionMap = {};
          // Clear existing options
          while (datalist.firstChild) {
            datalist.removeChild(datalist.firstChild);
          }
          data.forEach(item => {
            // Compose a human‑readable label for display in the datalist
            const label = item.label;
            suggestionMap[label] = item;
            const option = document.createElement('option');
            option.value = label;
            datalist.appendChild(option);
          });
        })
        .catch(err => {
          console.error('Errore durante il recupero dei suggerimenti di ricerca:', err);
        });
    });
    // Intercept form submission to redirect to the appropriate page based on the
    // selected suggestion.  If the entered value exactly matches one of the
    // suggestion labels, we build a URL pointing to the component table for the
    // corresponding product and category and include a highlight parameter.
    searchForm.addEventListener('submit', function(e){
      e.preventDefault();
      const value = searchInput.value.trim();
      if (value && suggestionMap[value]) {
        const item = suggestionMap[value];
        // Build the URL for the category table page with a highlight query
        const url = `/products/${item.product_id}/category/${item.category}/table?highlight=${item.component_id}`;
        window.location.href = url;
      }
    });
  }
});

// Production box interactions (load/build modals and DataMatrix download handling)
document.addEventListener('DOMContentLoaded', function(){
  const productionSection = document.querySelector('.production-box-section');
  if (!productionSection) {
    return;
  }

  const buildPanel = document.getElementById('build-inline-panel');
  const buildLoader = buildPanel ? buildPanel.querySelector('.build-inline-loader') : null;
  const buildContainer = buildPanel ? buildPanel.querySelector('[data-build-container]') : null;
  const buildBaseUrl = buildPanel ? buildPanel.getAttribute('data-build-url') : '';
  const buildBoxId = buildPanel ? buildPanel.getAttribute('data-box-id') : '';
  const buildCompleteOverlay = productionSection.querySelector('[data-build-complete-overlay]');
  const buildCompleteMessage = buildCompleteOverlay ? buildCompleteOverlay.querySelector('[data-build-complete-message]') : null;
  const buildCompleteDetail = buildCompleteOverlay ? buildCompleteOverlay.querySelector('[data-build-complete-detail]') : null;
  const buildCompleteDismiss = buildCompleteOverlay ? buildCompleteOverlay.querySelector('[data-build-complete-dismiss]') : null;
  const backUrl = productionSection.getAttribute('data-back-url') || '';
  const backLink = productionSection.querySelector('[data-back-link]');
  let buildRedirectTimer = null;

  if (backLink) {
    backLink.addEventListener('click', (evt) => {
      evt.preventDefault();
      navigateAfterBuild(backUrl);
    });
  }

  function navigateAfterBuild(targetUrl) {
    const destination = targetUrl || backUrl;
    if (destination) {
      window.location.href = destination;
    } else {
      window.location.reload();
    }
  }

  function showBuildCompletion(status, payload) {
    const detailText = payload && payload.detail
      ? payload.detail
      : "Ti riportiamo all'area produzione.";
    const titleText = payload && payload.message
      ? payload.message
      : (status === 'success' ? 'Costruzione completata' : 'Operazione aggiornata');
    if (buildCompleteOverlay) {
      buildCompleteOverlay.hidden = false;
      buildCompleteOverlay.classList.add('is-visible');
      if (buildCompleteMessage) {
        buildCompleteMessage.textContent = titleText;
      }
      if (buildCompleteDetail) {
        buildCompleteDetail.textContent = detailText;
      }
    }
    const redirectTarget = payload && payload.redirect ? payload.redirect : backUrl;
    if (buildRedirectTimer) {
      window.clearTimeout(buildRedirectTimer);
    }
    buildRedirectTimer = window.setTimeout(() => {
      navigateAfterBuild(redirectTarget);
    }, 1200);
    if (buildCompleteDismiss) {
      const handler = (evt) => {
        evt.preventDefault();
        if (buildRedirectTimer) {
          window.clearTimeout(buildRedirectTimer);
          buildRedirectTimer = null;
        }
        navigateAfterBuild(redirectTarget);
      };
      buildCompleteDismiss.addEventListener('click', handler, { once: true });
    }
  }

  window.handleInlineBuildComplete = function handleInlineBuildComplete(status, payload) {
    showBuildCompletion(status || 'success', payload || {});
  };

  function toggleBuildLoading(isLoading){
    if (!buildPanel || !buildLoader) {
      return;
    }
    buildPanel.setAttribute('data-state', isLoading ? 'loading' : 'ready');
    buildLoader.classList.toggle('is-visible', !!isLoading);
  }

  function executeEmbeddedScripts(scope){
    if (!scope) {
      return;
    }
    const scripts = scope.querySelectorAll('script');
    scripts.forEach((oldScript) => {
      const newScript = document.createElement('script');
      Array.from(oldScript.attributes).forEach((attr) => {
        newScript.setAttribute(attr.name, attr.value);
      });
      if (oldScript.textContent) {
        newScript.textContent = oldScript.textContent;
      }
      oldScript.replaceWith(newScript);
    });
  }

  function attachInlineBuildHandlers(root){
    if (!root) {
      return;
    }
    const form = root.querySelector('form');
    if (!form) {
      return;
    }
    form.addEventListener('submit', function(evt){
      evt.preventDefault();
      if (!buildContainer) {
        return;
      }
      const submitUrl = new URL(form.getAttribute('action') || buildBaseUrl, window.location.href);
      submitUrl.searchParams.set('fragment', '1');
      submitUrl.searchParams.set('inline', '1');
      if (buildBoxId) {
        submitUrl.searchParams.set('box_id', buildBoxId);
      }
      toggleBuildLoading(true);
      fetch(submitUrl.toString(), {
        method: 'POST',
        body: new FormData(form),
      }).then((resp) => {
        if (resp.redirected) {
          window.location.href = resp.url || window.location.href;
          return null;
        }
        const contentType = resp.headers.get('content-type') || '';
        if (contentType.includes('application/json')) {
          return resp.json();
        }
        return resp.text();
      }).then((payload) => {
        if (!payload) {
          return;
        }
        if (typeof payload === 'object' && !(payload instanceof String)) {
          if (payload.status === 'success') {
            if (typeof window.handleInlineBuildComplete === 'function') {
              window.handleInlineBuildComplete('success', payload);
            } else {
              navigateAfterBuild(payload.redirect || backUrl);
            }
            return;
          }
          if (payload.html) {
            buildContainer.innerHTML = payload.html;
            executeEmbeddedScripts(buildContainer);
            attachInlineBuildHandlers(buildContainer);
          }
          return;
        }
        buildContainer.innerHTML = payload;
        executeEmbeddedScripts(buildContainer);
        attachInlineBuildHandlers(buildContainer);
      }).catch(() => {
        // Leave current content intact on error
      }).finally(() => {
        toggleBuildLoading(false);
      });
    });
  }

  function loadBuildFragment(){
    if (!buildContainer || !buildBaseUrl) {
      return;
    }
    const url = new URL(buildBaseUrl, window.location.href);
    url.searchParams.set('fragment', '1');
    url.searchParams.set('inline', '1');
    if (buildBoxId) {
      url.searchParams.set('box_id', buildBoxId);
    }
    toggleBuildLoading(true);
    fetch(url.toString(), {
      headers: { 'X-Requested-With': 'XMLHttpRequest' },
    }).then((resp) => resp.text())
      .then((html) => {
        buildContainer.innerHTML = html;
        executeEmbeddedScripts(buildContainer);
        attachInlineBuildHandlers(buildContainer);
      })
      .catch(() => {
        buildContainer.innerHTML = '<p class="muted" style="margin:0;">Impossibile caricare la procedura di costruzione.</p>';
      })
      .finally(() => {
        toggleBuildLoading(false);
      });
  }

  if (buildPanel && buildContainer && buildBaseUrl) {
    loadBuildFragment();
  }

  const inlineLoadForm = document.getElementById('inline-load-form');
  const inlineLoadStatus = inlineLoadForm ? inlineLoadForm.querySelector('[data-load-status]') : null;
  const inlineLoadSubmit = inlineLoadForm ? inlineLoadForm.querySelector('[data-load-submit]') : null;
  const inlineSelectionHelper = inlineLoadForm ? inlineLoadForm.querySelector('[data-selection-helper]') : null;
  const inlineItemInput = inlineLoadForm ? inlineLoadForm.querySelector('input[name="item_id"]') : null;
  const inlineFileInputs = inlineLoadForm ? inlineLoadForm.querySelectorAll('[data-load-input]') : [];
  const inlineLoadLotMode = inlineLoadForm ? (inlineLoadForm.getAttribute('data-lot-mode') === 'true') : false;
  const inlineSelectButtons = document.querySelectorAll('[data-select-item]');
  const tableRows = document.querySelectorAll('.production-box-table tbody tr');

  if (inlineLoadLotMode && inlineItemInput) {
    inlineItemInput.value = '';
  }

  function updateInlineLoadStatus(){
    const docsOk = inlineFileInputs && inlineFileInputs.length > 0
      ? Array.from(inlineFileInputs).every((inp) => inp.files && inp.files.length > 0)
      : true;
    const itemSelected = inlineLoadLotMode ? true : Boolean(inlineItemInput && inlineItemInput.value);
    if (inlineLoadStatus) {
      inlineLoadStatus.textContent = docsOk ? 'Documenti pronti' : 'Documenti mancanti';
      inlineLoadStatus.classList.toggle('is-ready', docsOk);
      inlineLoadStatus.classList.toggle('is-pending', !docsOk);
    }
    if (inlineLoadSubmit) {
      inlineLoadSubmit.disabled = !docsOk || !itemSelected;
    }
  }

  function updateFileNameDisplay(input){
    if (!input) {
      return;
    }
    const container = input.parentElement;
    if (!container) {
      return;
    }
    const nameSpan = container.querySelector('.document-file-name');
    if (!nameSpan) {
      return;
    }
    if (input.files && input.files.length > 0) {
      nameSpan.textContent = input.files[0].name;
    } else {
      nameSpan.textContent = '';
    }
  }

  if (inlineFileInputs && inlineFileInputs.length > 0) {
    inlineFileInputs.forEach((input) => {
      input.addEventListener('change', () => {
        updateFileNameDisplay(input);
        updateInlineLoadStatus();
      });
    });
  }
  updateInlineLoadStatus();

  function clearRowHighlights(){
    if (!tableRows) {
      return;
    }
    tableRows.forEach((row) => {
      row.classList.remove('is-targeted');
    });
  }

  function highlightRowForButton(btn){
    if (!btn) {
      return;
    }
    const row = btn.closest('tr');
    clearRowHighlights();
    if (row) {
      row.classList.add('is-targeted');
    }
  }

  function applySelection(itemId, label, sourceBtn){
    if (inlineLoadLotMode) {
      updateInlineLoadStatus();
      return;
    }
    if (inlineItemInput) {
      inlineItemInput.value = itemId || '';
    }
    if (inlineSelectionHelper) {
      inlineSelectionHelper.textContent = itemId
        ? `Caricamento per ${label || `ID ${itemId}`}`
        : 'Seleziona un componente dalla tabella per associare i documenti caricati.';
    }
    if (sourceBtn) {
      highlightRowForButton(sourceBtn);
    }
    updateInlineLoadStatus();
    if (inlineLoadForm) {
      inlineLoadForm.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  }

  if (!inlineLoadLotMode && inlineSelectButtons && inlineSelectButtons.length > 0) {
    inlineSelectButtons.forEach((btn) => {
      btn.addEventListener('click', (evt) => {
        evt.preventDefault();
        const itemId = btn.getAttribute('data-item-id') || '';
        const itemLabel = btn.getAttribute('data-item-label') || '';
        applySelection(itemId, itemLabel, btn);
      });
    });
  }

  if (!inlineLoadLotMode && tableRows && tableRows.length > 0) {
    tableRows.forEach((row) => {
      row.addEventListener('click', (evt) => {
        if (evt.target && evt.target.closest('button')) {
          return;
        }
        const btn = row.querySelector('[data-select-item]');
        if (btn) {
          const itemId = btn.getAttribute('data-item-id') || '';
          const itemLabel = btn.getAttribute('data-item-label') || '';
          applySelection(itemId, itemLabel, btn);
        }
      });
    });
  }

  if (inlineLoadForm) {
    const triggerButtons = inlineLoadForm.querySelectorAll('[data-load-trigger-button]');
    if (triggerButtons && triggerButtons.length > 0) {
      triggerButtons.forEach((btn) => {
        btn.addEventListener('click', (evt) => {
          evt.preventDefault();
          const wrapper = btn.parentElement;
          if (!wrapper) {
            return;
          }
          const input = wrapper.querySelector('input[type="file"]');
          if (input) {
            input.click();
          }
        });
      });
    }

    inlineLoadForm.addEventListener('submit', (evt) => {
      if (inlineLoadSubmit && inlineLoadSubmit.disabled) {
        evt.preventDefault();
      }
    });
  }

  function triggerDownload(blob, filename){
    const link = document.createElement('a');
    const objectUrl = URL.createObjectURL(blob);
    link.href = objectUrl;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(objectUrl);
  }

  function base64ToBlob(base64Data){
    const clean = (base64Data || '').replace(/\s+/g, '');
    const binary = atob(clean);
    const len = binary.length;
    const buffer = new ArrayBuffer(len);
    const view = new Uint8Array(buffer);
    for (let i = 0; i < len; i += 1) {
      view[i] = binary.charCodeAt(i);
    }
    return new Blob([buffer], { type: 'image/png' });
  }

  function svgToPngDataUrl(svgElement, size){
    return new Promise((resolve, reject) => {
      try {
        const svgString = new XMLSerializer().serializeToString(svgElement);
        const blob = new Blob([svgString], { type: 'image/svg+xml;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const img = new Image();
        const canvas = document.createElement('canvas');
        const dimension = size || Math.max(svgElement.clientWidth, svgElement.clientHeight, 240);
        canvas.width = dimension;
        canvas.height = dimension;
        img.onload = function(){
          const ctx = canvas.getContext('2d');
          if (!ctx) {
            URL.revokeObjectURL(url);
            reject(new Error('Contesto canvas non disponibile'));
            return;
          }
          ctx.clearRect(0, 0, dimension, dimension);
          ctx.drawImage(img, 0, 0, dimension, dimension);
          URL.revokeObjectURL(url);
          try {
            resolve(canvas.toDataURL('image/png'));
          } catch (err) {
            reject(err);
          }
        };
        img.onerror = function(err){
          URL.revokeObjectURL(url);
          reject(err);
        };
        img.src = url;
      } catch (err) {
        reject(err);
      }
    });
  }

  function renderDataMatrix(){
    const containers = document.querySelectorAll('.dm-container[data-dm-code]');
    if (!containers || containers.length === 0) {
      return;
    }
    containers.forEach((container) => {
      const code = container.getAttribute('data-dm-code') || '';
      const trimmed = code.trim();
      if (!trimmed) {
        return;
      }
      if (typeof window !== 'undefined' && typeof window.DATAMatrix === 'function') {
        try {
          const svg = window.DATAMatrix({ msg: trimmed, dim: 72, pad: 1, pal: ['#000', '#fff'] });
          const existing = container.querySelector('svg');
          if (existing && existing.parentNode) {
            existing.parentNode.removeChild(existing);
          }
          if (svg) {
            svg.setAttribute('role', 'img');
            svg.setAttribute('aria-label', `Datamatrix ${trimmed}`);
            container.prepend(svg);
            container.classList.add('is-rendered');
          }
        } catch (err) {
          console.warn('Impossibile generare il DataMatrix in locale', err);
        }
      }
    });
  }

  renderDataMatrix();
  window.addEventListener('load', renderDataMatrix);

  let dmEnsureAttempts = 0;
  const dmEnsureMax = 15;
  (function ensureMatrixReady(){
    if (typeof window !== 'undefined' && typeof window.DATAMatrix === 'function') {
      renderDataMatrix();
      return;
    }
    dmEnsureAttempts += 1;
    if (dmEnsureAttempts < dmEnsureMax) {
      setTimeout(ensureMatrixReady, 150);
    }
  })();

  const downloadButtons = document.querySelectorAll('.dm-download');
  if (downloadButtons && downloadButtons.length > 0) {
    downloadButtons.forEach((btn) => {
      btn.addEventListener('click', function(evt){
        evt.preventDefault();
        const baseName = btn.getAttribute('data-base-name') || 'datamatrix';
        const trimmedName = baseName ? baseName.toString().trim() : '';
        const safeBase = trimmedName ? trimmedName.replace(/[^a-zA-Z0-9-_]+/g, '_') : 'datamatrix';
        const encoded = btn.getAttribute('data-dm-image') || '';
        const payload = btn.getAttribute('data-dm-code') || '';
        const row = btn.closest('tr');
        const container = row ? row.querySelector('.dm-container') : null;
        const svg = container ? container.querySelector('svg') : null;
        if (svg) {
          svgToPngDataUrl(svg, 360).then((dataUrl) => {
            const link = document.createElement('a');
            link.href = dataUrl;
            link.download = safeBase + '-datamatrix.png';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
          }).catch((err) => {
            console.error('Unable to export generated DataMatrix', err);
          });
          return;
        }
        if (encoded) {
          try {
            const blob = base64ToBlob(encoded);
            const fileName = safeBase + '-datamatrix.png';
            triggerDownload(blob, fileName);
            return;
          } catch (err) {
            console.error('Unable to decode pre-rendered DataMatrix', err);
          }
        }
        if (payload) {
          const fallbackBlob = new Blob([payload], { type: 'text/plain;charset=utf-8' });
          const fallbackName = safeBase + '-datamatrix.txt';
          triggerDownload(fallbackBlob, fallbackName);
          return;
        }
        alert('Impossibile scaricare il Datamatrix.');
      });
    });
  }
});
