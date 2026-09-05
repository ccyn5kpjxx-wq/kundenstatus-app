(() => {
 const nav=document.querySelector('.admin-sidebar');
 if(!nav)return;
 const open=document.querySelector('.sidebar-open'),close=nav.querySelector('.sidebar-close');
 const mobile=matchMedia('(max-width:991px)');
 function sync(){open.setAttribute('aria-expanded',String(mobile.matches ? document.body.classList.contains('sidebar-mobile-open') : !document.body.classList.contains('sidebar-hidden')));}
 function hide(){document.body.classList.remove('sidebar-mobile-open');if(!mobile.matches){document.body.classList.add('sidebar-hidden');try{localStorage.setItem('werkstatt-menu-hidden','1')}catch{}}sync();open.focus();}
 try{if(localStorage.getItem('werkstatt-menu-hidden')==='1'&&!mobile.matches)document.body.classList.add('sidebar-hidden')}catch{}
 open.addEventListener('click',()=>{document.body.classList.remove('sidebar-hidden');document.body.classList.add('sidebar-mobile-open');try{localStorage.removeItem('werkstatt-menu-hidden')}catch{}sync();close.focus()});
 close.addEventListener('click',hide);
 document.addEventListener('keydown',e=>{if(e.key==='Escape'&&nav.contains(document.activeElement))hide()});
 mobile.addEventListener('change',()=>{document.body.classList.remove('sidebar-mobile-open');sync()});sync();
})();
