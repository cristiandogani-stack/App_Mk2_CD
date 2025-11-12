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
