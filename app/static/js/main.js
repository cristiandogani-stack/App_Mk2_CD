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

  const loadModal = document.getElementById('loadModal');
  const loadFrame = document.getElementById('loadFrame');
  const buildModal = document.getElementById('buildModal');
  const buildFrame = document.getElementById('buildFrame');
  const boxId = productionSection.getAttribute('data-box-id') || '';
  const sectionLoadBase = productionSection.getAttribute('data-load-base') || '';
  const modalLoadBase = loadModal ? (loadModal.getAttribute('data-base-url') || '') : '';
  const defaultLoadUrl = modalLoadBase || sectionLoadBase;

  function setModalLoading(modal, isLoading){
    if (!modal) { return; }
    if (isLoading) {
      modal.classList.add('is-loading');
      modal.setAttribute('aria-busy', 'true');
    } else {
      modal.classList.remove('is-loading');
      modal.setAttribute('aria-busy', 'false');
    }
  }

  function resetLoadModal(){
    if (loadFrame) {
      loadFrame.onload = null;
      loadFrame.src = '';
    }
    if (loadModal) {
      setModalLoading(loadModal, false);
      loadModal.style.display = 'none';
      loadModal.setAttribute('aria-hidden', 'true');
    }
  }

  function attachLoadWatcher(){
    if (!loadFrame || !loadModal) {
      return;
    }
    loadFrame.onload = function(){
      setModalLoading(loadModal, false);
      try {
        const loc = loadFrame.contentWindow.location;
        const href = loc && loc.href ? loc.href : '';
        const path = loc && loc.pathname ? loc.pathname : '';
        if (path && !path.startsWith('/inventory/load') && !href.startsWith('about:')) {
          resetLoadModal();
          window.location.reload();
        }
      } catch (err) {
        // Ignore errors caused by blank or cross-origin frames
      }
    };
  }

  function resolveLoadUrl(trigger, options){
    const candidate = (options && options.baseUrl) || (trigger ? trigger.getAttribute('data-load-url') : null) || defaultLoadUrl;
    if (!candidate) {
      return '';
    }
    const params = new URLSearchParams();
    params.set('embedded', '1');
    if (boxId) {
      params.set('box_id', boxId);
    }
    if (options && options.itemId) {
      params.set('item_id', options.itemId);
    }
    if (options && options.autoSelect) {
      params.set('auto_select', '1');
    }
    try {
      const url = new URL(candidate, window.location.origin);
      params.forEach((value, key) => {
        url.searchParams.set(key, value);
      });
      let finalUrl = url.pathname + url.search;
      if (url.hash) {
        finalUrl += url.hash;
      }
      return finalUrl;
    } catch (err) {
      const sep = candidate.indexOf('?') === -1 ? '?' : '&';
      return candidate + sep + params.toString();
    }
  }

  function openLoadModal(trigger, options){
    const targetUrl = resolveLoadUrl(trigger, options);
    if (!loadModal || !loadFrame) {
      if (targetUrl) {
        window.location.href = targetUrl;
      } else {
        alert('Impossibile aprire il caricamento per questo box.');
      }
      return;
    }
    if (!targetUrl) {
      alert('Impossibile aprire il caricamento per questo box.');
      return;
    }
    attachLoadWatcher();
    setModalLoading(loadModal, true);
    loadFrame.src = targetUrl;
    loadModal.style.display = 'flex';
    loadModal.setAttribute('aria-hidden', 'false');
  }

  const btnOpenLoad = document.getElementById('btn-open-load');
  if (btnOpenLoad) {
    btnOpenLoad.addEventListener('click', function(){
      openLoadModal(btnOpenLoad, { autoSelect: true });
    });
  }

  const btnCloseLoad = document.getElementById('btn-close-load');
  if (btnCloseLoad) {
    btnCloseLoad.addEventListener('click', function(){
      resetLoadModal();
    });
  }

  const btnBuild = document.getElementById('btn-open-build');
  if (btnBuild) {
    btnBuild.addEventListener('click', function(){
      const buildUrl = btnBuild.getAttribute('data-build-url');
      if (!buildUrl) {
        alert('Impossibile avviare la costruzione per questo box.');
        return;
      }
      const modal = buildModal;
      const iframe = buildFrame;
      if (!modal || !iframe) {
        window.location.href = buildUrl;
        return;
      }
      setModalLoading(modal, true);
      iframe.src = buildUrl;
      modal.style.display = 'flex';
      modal.setAttribute('aria-hidden', 'false');
      iframe.onload = function(){
        setModalLoading(modal, false);
        try {
          const loc = iframe.contentWindow.location;
          const href = loc && loc.href ? loc.href : '';
          const path = loc && loc.pathname ? loc.pathname : '';
          const isBuildPath = path && (path.startsWith('/inventory/build') || path.startsWith('/inventory/build_product'));
          if (!isBuildPath && href && !href.startsWith('about:')) {
            iframe.onload = null;
            modal.style.display = 'none';
            modal.setAttribute('aria-hidden', 'true');
            iframe.src = '';
            alert('Costruzione completata con successo');
            window.location.reload();
          }
        } catch (err) {
          // Ignore cross-origin or sandbox errors
        }
      };
    });
  }

  const btnCloseBuild = document.getElementById('btn-close-build');
  if (btnCloseBuild) {
    btnCloseBuild.addEventListener('click', function(){
      const modal = buildModal;
      const iframe = buildFrame;
      if (iframe) {
        iframe.onload = null;
        iframe.src = '';
      }
      if (modal) {
        setModalLoading(modal, false);
        modal.style.display = 'none';
        modal.setAttribute('aria-hidden', 'true');
      }
    });
  }

  const perItemButtons = document.querySelectorAll('.load-item');
  if (perItemButtons && perItemButtons.length > 0) {
    perItemButtons.forEach((btn) => {
      btn.addEventListener('click', function(evt){
        evt.stopPropagation();
        const itemId = this.getAttribute('data-item-id');
        if (itemId) {
          openLoadModal(this, { itemId: itemId, autoSelect: true });
        }
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
  window.addEventListener('load', renderDataMatrix, { once: true });

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
