(() => {
 const nav=document.querySelector('.admin-sidebar');
 if(!nav)return;
 const currentGroup = nav.querySelector('.is-active, [aria-current="page"]')?.closest('.sidebar-group');
 if (currentGroup) currentGroup.open = true;
 const open=document.querySelector('.sidebar-open'),close=nav.querySelector('.sidebar-close');
 const mobile=matchMedia('(max-width:991px)');
 function sync(){open.setAttribute('aria-expanded',String(mobile.matches ? document.body.classList.contains('sidebar-mobile-open') : !document.body.classList.contains('sidebar-hidden')));}
 function hide(){document.body.classList.remove('sidebar-mobile-open');if(!mobile.matches){document.body.classList.add('sidebar-hidden');try{localStorage.setItem('werkstatt-menu-hidden','1')}catch{}}sync();open.focus();}
 try{if(localStorage.getItem('werkstatt-menu-hidden')==='1'&&!mobile.matches)document.body.classList.add('sidebar-hidden')}catch{}
 open.addEventListener('click',()=>{document.body.classList.remove('sidebar-hidden');document.body.classList.add('sidebar-mobile-open');try{localStorage.removeItem('werkstatt-menu-hidden')}catch{}sync();close.focus()});
 close.addEventListener('click',hide);
 document.addEventListener('keydown',e=>{if(e.key==='Escape'&&nav.contains(document.activeElement))hide()});
 mobile.addEventListener('change',()=>{document.body.classList.remove('sidebar-mobile-open');sync()});sync();

 const badge = nav.querySelector('[data-shop-pending-count]');
 if (!badge) return;
 let loading = false;
 async function refreshOrderCount() {
   if (document.hidden || loading) return;
   loading = true;
   let count = null;
   try {
     const response = await fetch(badge.dataset.countUrl, {
       mode: 'same-origin', cache: 'no-store', headers: {Accept: 'application/json'},
       signal: AbortSignal.timeout(10000)
     });
     if (response.ok) {
       const result = await response.json();
       if (Number.isSafeInteger(result.count) && result.count >= 0) count = result.count;
     }
   } catch {
     // The next refresh retries after an interrupted connection.
   } finally {
     loading = false;
   }
   badge.textContent = count === null ? '–' : String(count);
   badge.title = count === null ? 'Anzahl derzeit nicht verfügbar' : 'Noch zu bearbeiten';
   badge.classList.toggle('is-alert', count > 0);
   badge.closest('a').setAttribute('aria-label', ['Shopbestellungen,', badge.title, badge.textContent].join(' '));
 }
 window.setInterval(refreshOrderCount, 60000);
 document.addEventListener('visibilitychange', refreshOrderCount);
 window.addEventListener('pageshow', event => { if (event.persisted) refreshOrderCount(); });
})();
