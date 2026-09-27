(() => {
  'use strict';
  const menu = document.getElementById('assistant-menu');
  if (!menu) return;
  const open = () => { if (!menu.open) menu.showModal(); };
  document.getElementById('menu-open').addEventListener('click',open);
  document.getElementById('active-order').addEventListener('click',()=>{open();menu.querySelector('details').open=true;document.getElementById('order-form').scrollIntoView({block:'center'});});
  document.getElementById('menu-close').addEventListener('click',()=>menu.close());
  const cards = document.getElementById('overview-cards');
  const status = document.getElementById('overview-status');
  const titles = {morgen:'Heute wichtig',raus:'Heute fertig / raus',rein:'Kommt heute',lack:'Lackierung & Farbcodes'};
  let downloadText = '';
  let generation = 0;
  const labels = {heute_faellig:'Heute fertigstellen',ueberfaellig:'Fertigstellung überfällig',anlieferung_heute:'Kunde bringt',abholung_durch_werkstatt_heute:'Bei Partner abholen',rueckbringung_heute:'Zurückbringen',kundenabholung_heute:'Kunde holt ab'};
  async function show(view) {
    const run = ++generation;
    document.getElementById('overview-panel').hidden=false;
    document.getElementById('overview-title').textContent=titles[view];
    document.getElementById('paint-period-label').hidden=view!=='lack';
    status.textContent='Aktuellen Cockpit-Stand laden …';cards.replaceChildren();
    document.getElementById('overview-download').hidden=true;
    try {
      const period=document.getElementById('paint-period').value;
      const response=await fetch('/werkstatt/assistent/ueberblick?ansicht='+encodeURIComponent(view)+'&zeitraum='+encodeURIComponent(period));
      const data=await response.json();
      if (!response.ok) throw new Error(data.error || 'Tagesübersicht ist gerade nicht erreichbar.');
      if (run !== generation) return;
      const rows=data.eintraege || [];
      status.textContent=data.hinweis || `${rows.length} Einträge · aktueller Cockpit-Stand`;
      const texts=[titles[view],data.datum || '',status.textContent];
      for (const item of rows) {
        const card=document.createElement('article');card.className='overview-card';
        const heading=document.createElement('h3');heading.textContent=`Auftrag ${item.auftrag_id || item.id} · ${item.fahrzeug || 'Fahrzeug'}`;
        const details=document.createElement('p');
        details.textContent=[labels[item.art] || item.art || '',item.kennzeichen || '',item.autohaus || '',item.datum || '',item.uhrzeit ? `Uhrzeit: ${item.uhrzeit}` : 'Keine Uhrzeit hinterlegt',item.hinweis || ''].filter(Boolean).join(' · ');
        card.append(heading,details);texts.push(heading.textContent,details.textContent);
        if (view==='lack') {
          const code=document.createElement('p');code.className='overview-code';code.textContent=`Farbcode: ${item.farbcode || 'nicht hinterlegt'}`;
          const shades=document.createElement('p');shades.textContent=`Farbton: ${item.farbton || 'nicht hinterlegt'}${item.farbton_2 ? ' · '+item.farbton_2 : ''}`;
          card.append(code,shades);texts.push(code.textContent,shades.textContent);
        }
        const button=document.createElement('button');button.type='button';button.className='secondary';button.textContent='Auftrag ansehen';
        button.addEventListener('click',()=>{
          menu.querySelector('details').open=true;
          document.dispatchEvent(new CustomEvent('assistant-open-order',{detail:{id:item.auftrag_id || item.id}}));
        });
        card.append(button);cards.append(card);
      }
      if (!rows.length) { const empty=document.createElement('p');empty.textContent='Keine passenden Einträge im aktuellen Cockpit-Stand.';cards.append(empty); }
      downloadText=texts.join('\n\n');document.getElementById('overview-download').hidden=false;
    } catch(error) { if (run===generation) status.textContent=error.message; }
  }
  menu.querySelectorAll('[data-overview]').forEach(button=>button.addEventListener('click',()=>show(button.dataset.overview)));
  document.getElementById('paint-period').addEventListener('change',()=>show('lack'));
  document.getElementById('overview-download').addEventListener('click',()=>{
    const url=URL.createObjectURL(new Blob([downloadText],{type:'text/plain;charset=utf-8'}));
    const a=document.createElement('a');a.href=url;a.download='Werkstatt-Uebersicht.txt';a.click();
    setTimeout(()=>URL.revokeObjectURL(url),1000);
  });
})();
