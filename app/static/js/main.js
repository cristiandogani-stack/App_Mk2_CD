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
  const buildFrame = document.getElementById('buildFrame');
  const buildLoader = buildPanel ? buildPanel.querySelector('.build-inline-loader') : null;
  const buildUrl = buildPanel ? buildPanel.getAttribute('data-build-url') : '';

  function beginBuildLoad(){
    if (!buildPanel) {
      return;
    }
    buildPanel.setAttribute('data-state', 'loading');
    if (buildLoader) {
      buildLoader.classList.add('is-visible');
    }
  }

  function markBuildReady(){
    if (!buildPanel) {
      return;
    }
    buildPanel.setAttribute('data-state', 'ready');
    if (buildLoader) {
      buildLoader.classList.remove('is-visible');
    }
  }

  function resetBuildFrame(){
    if (!buildFrame) {
      return;
    }
    buildFrame.removeAttribute('data-active-url');
    buildFrame.src = 'about:blank';
  }

  function loadBuildFrame(targetUrl, options){
    if (!buildPanel || !buildFrame || !targetUrl) {
      return;
    }
    beginBuildLoad();
    const cacheKey = buildFrame.getAttribute('data-active-url');
    const sameTarget = cacheKey && cacheKey === targetUrl && !(options && options.forceReload);
    const finalUrl = targetUrl + (targetUrl.indexOf('?') === -1 ? '?' : '&') + 'ts=' + Date.now();
    buildFrame.setAttribute('data-active-url', targetUrl);
    if (!sameTarget) {
      buildFrame.src = finalUrl;
    } else if (buildLoader) {
      buildLoader.classList.remove('is-visible');
      buildPanel.setAttribute('data-state', 'ready');
    }
  }

  if (buildFrame) {
    buildFrame.addEventListener('load', function(){
      markBuildReady();
    });
  }

  if (buildUrl) {
    loadBuildFrame(buildUrl, { forceReload: true });
  }

  if (typeof window.handleEmbeddedBuildComplete !== 'function') {
    window.handleEmbeddedBuildComplete = function(status){
      resetBuildFrame();
      if (status === 'success') {
        window.location.reload();
      }
    };
  }

  const inlineLoadPanel = document.getElementById('load-inline-panel');
  const inlineLoadForm = document.getElementById('inline-load-form');
  const inlineLoadStatus = inlineLoadPanel ? inlineLoadPanel.querySelector('[data-load-status]') : null;
  const inlineLoadSubmit = inlineLoadForm ? inlineLoadForm.querySelector('[data-load-submit]') : null;
  const inlineSelectionWrapper = inlineLoadPanel ? inlineLoadPanel.querySelector('[data-selection-wrapper]') : null;
  const inlineSelectionLabel = inlineLoadPanel ? inlineLoadPanel.querySelector('[data-selected-label]') : null;
  const inlineItemInput = inlineLoadForm ? inlineLoadForm.querySelector('input[name="item_id"]') : null;
  const inlineFileInputs = inlineLoadForm ? inlineLoadForm.querySelectorAll('input[type="file"]') : [];
  const inlineRequiresDocs = inlineFileInputs && inlineFileInputs.length > 0;
  const loadTriggers = document.querySelectorAll('[data-load-trigger]');
  const tableRows = document.querySelectorAll('.production-box-table tbody tr');

  function updateInlineLoadStatus(){
    let docsOk = true;
    if (inlineFileInputs && inlineFileInputs.length > 0) {
      docsOk = Array.from(inlineFileInputs).every((inp) => inp.files && inp.files.length > 0);
    }
    if (inlineLoadStatus) {
      inlineLoadStatus.textContent = docsOk ? '🟢 Documenti pronti' : '🔴 Documenti mancanti';
    }
    if (inlineLoadSubmit) {
      inlineLoadSubmit.disabled = inlineRequiresDocs && !docsOk;
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
      input.addEventListener('change', function(){
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

  function focusFirstInput(){
    if (!inlineLoadForm) {
      return;
    }
    const firstInput = inlineLoadForm.querySelector('input[type="file"]');
    if (firstInput) {
      try {
        firstInput.focus({ preventScroll: true });
      } catch (err) {
        firstInput.focus();
      }
      if ((!firstInput.files || firstInput.files.length === 0)) {
        try {
          firstInput.click();
        } catch (err) {
          // ignore
        }
      }
    }
  }

  if (loadTriggers && loadTriggers.length > 0) {
    loadTriggers.forEach((btn) => {
      btn.addEventListener('click', function(evt){
        evt.preventDefault();
        const itemId = btn.getAttribute('data-item-id') || '';
        const itemLabel = btn.getAttribute('data-item-label') || (itemId ? `ID ${itemId}` : 'intero box');
        if (inlineItemInput) {
          inlineItemInput.value = itemId;
        }
        if (inlineSelectionWrapper && inlineSelectionLabel) {
          if (itemId) {
            inlineSelectionLabel.textContent = itemLabel;
            inlineSelectionWrapper.hidden = false;
          } else {
            inlineSelectionWrapper.hidden = true;
            inlineSelectionLabel.textContent = '';
          }
        }
        highlightRowForButton(btn);
        if (inlineLoadPanel) {
          inlineLoadPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
        focusFirstInput();
      });
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
