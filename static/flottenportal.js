(() => {
  'use strict';
  const search=document.querySelector('[data-fleet-search]');
  const filterVehicles=()=>{
    let visible=0;
    document.querySelectorAll('[data-fleet-card]').forEach(card=>{
      card.hidden=!card.dataset.search.includes(search ? search.value.trim().toLowerCase() : '');
      if(!card.hidden) visible++;
    });
    const empty=document.querySelector('[data-search-empty]');
    if(empty) empty.hidden=visible>0;
  };
  if(search) search.addEventListener('input',filterVehicles);
  const live=document.querySelector('[data-fleet-live]');
  if(live) {
    const root=document.querySelector('[data-private-main]'), note=live.querySelector('[data-status-note]');
    const expectedView=root.dataset.portalView;
    let busy=false, stopped=false, controller=null;
    const node=(tag,css,text)=>{
      const element=document.createElement(tag);
      if(css) element.className=css;
      if(text!==undefined) element.textContent=text;
      return element;
    };
    const clearPrivate=()=>{
      stopped=true;
      document.title='Bitte anmelden · Mein Fuhrpark';
      if(controller) controller.abort();
      document.querySelectorAll('.fp-header form').forEach(form=>form.remove());
      const message=node('section','fp-card');
      message.append(node('h1','','Bitte erneut anmelden'),node('p','fp-note','Der Zugang oder die Fahrzeugzuordnung hat sich geändert. Zum Schutz Ihrer Daten wurde diese Ansicht geschlossen.'));
      const link=node('a','fp-primary','Zur Anmeldung →');
      link.href='/flotte/anmelden';message.append(link);
      root.replaceChildren(message);
    };
    const validCase=c=>c && Number.isInteger(c.id) && Number.isInteger(c.status) &&
      ['title','label','description','updated','stage'].every(k=>typeof c[k]==='string') &&
      Array.isArray(c.steps) && c.steps.length===5 && c.steps.every(s=>typeof s.label==='string' && typeof s.done==='boolean' && typeof s.current==='boolean');
    const renderCase=c=>{
      const card=node('article','kf-case');card.dataset.caseCard='';card.dataset.caseId=c.id;
      const head=node('div','kf-case-head'), info=node('div','');
      const icon=node('span','kf-case-icon');icon.setAttribute('aria-hidden','true');
      const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');
      Object.entries({viewBox:'0 0 24 24',fill:'none',stroke:'currentColor','stroke-width':'1.7','stroke-linecap':'round','stroke-linejoin':'round'}).forEach(([key,value])=>svg.setAttribute(key,value));
      const path=document.createElementNS('http://www.w3.org/2000/svg','path');
      path.setAttribute('d','M14.6 6.4a5 5 0 0 0-6.5 6.5L3 18a2.1 2.1 0 0 0 3 3l5.1-5.1a5 5 0 0 0 6.5-6.5l-3.3 3.3-3-3z');
      svg.append(path);icon.append(svg);
      info.append(node('p','kf-case-kicker','IHR WERKSTATTFALL'),node('h2','',c.title));
      head.append(icon,info,node('span','kf-case-state',c.label));card.append(head,node('p','kf-case-description',c.description));
      if(c.stage) card.append(node('p','kf-case-stage',c.stage));
      const steps=node('ol','kf-case-steps');steps.setAttribute('aria-label','Bearbeitungsstand');
      c.steps.forEach((s,i)=>{
        const item=node('li',s.done?'done':s.current?'current':'');
        if(s.current) item.setAttribute('aria-current','step');
        const number=node('span','',String(i+1));number.setAttribute('aria-hidden','true');
        item.append(number,node('b','',s.label));steps.append(item);
      });
      const foot=node('div','kf-case-foot'), time=node('time','',c.updated), update=node('span','','Letztes Update: ');
      update.append(time);foot.append(update,node('span','','Stand aus dem Werkstattauftrag'));
      card.append(steps,foot);return card;
    };
    const refresh=async()=>{
      if(busy || stopped || document.hidden) return;
      busy=true;controller=new AbortController();
      const timeout=setTimeout(()=>controller.abort(),10000);
      const button=document.querySelector('[data-status-refresh]');
      button?.setAttribute('aria-busy','true');
      try {
        const url=new URL(live.dataset.statusUrl,location.href);
        if(url.origin!==location.origin) throw new Error('origin');
        const response=await fetch(url,{credentials:'same-origin',cache:'no-store',headers:{Accept:'application/json'},signal:controller.signal,redirect:'error'});
        if([401,403,404].includes(response.status)){clearPrivate();return;}
        if(!response.ok) throw new Error('status');
        const data=await response.json();
        if(stopped || document.hidden) return;
        if(data.view!==expectedView){clearPrivate();return;}
        let changed=false;
        if(Array.isArray(data.vehicles)) {
          if(!data.vehicles.every(v=>typeof v.id==='string' && Number.isInteger(v.offen) && Array.isArray(v.cases) && v.cases.every(validCase))) throw new Error('shape');
          const cards=[...document.querySelectorAll('[data-fleet-card]')];
          const ids=new Set(data.vehicles.map(v=>v.id));
          cards.filter(card=>!ids.has(card.dataset.vehicleId)).forEach(card=>card.remove());
          data.vehicles.forEach(v=>{
            const card=cards.find(card=>card.dataset.vehicleId===v.id);
            if(!card){changed=true;return;}
            const area=card.querySelector('[data-card-cases]');
            area.replaceChildren(...(v.cases.length?[node('span','fp-pill',v.cases[0].label),node('p','',v.cases[0].title)]:[node('span','fp-neutral','Kein aktueller Werkstattfall')]));
            const due=card.querySelector('[data-card-due]');due.hidden=!v.offen;due.textContent=v.offen+' Fälligkeit(en) prüfen';
          });
          document.querySelector('[data-stat-vehicles]').textContent=data.vehicles.length;
          document.querySelector('[data-stat-cases]').textContent=data.vehicles.filter(v=>v.cases.length).length;
          document.querySelector('[data-stat-due]').textContent=data.vehicles.reduce((sum,v)=>sum+v.offen,0);
          filterVehicles();
        } else if(Array.isArray(data.cases) && data.cases.every(validCase)) {
          const area=document.querySelector('[data-fleet-cases]');
          area.replaceChildren(...(data.cases.length?data.cases.map(renderCase):[node('p','fp-card fp-note','Aktuell ist kein Werkstattfall für dieses Fahrzeug hinterlegt.')]));
        } else throw new Error('shape');
        note.classList.remove('fp-warning');
        note.textContent='Zuletzt geprüft: '+new Date().toLocaleTimeString('de-DE',{hour:'2-digit',minute:'2-digit',second:'2-digit'})+' · Automatische Prüfung alle 30 Sekunden.'+(changed?' Neue Fahrzeuge vorhanden – bitte „Alle Fahrzeuge“ bzw. die Übersicht neu öffnen.':'');
      } catch(error) {
        if(!stopped && !document.hidden){note.classList.add('fp-warning');note.textContent='Aktualisierung zurzeit nicht möglich. Der angezeigte Werkstattstand kann veraltet sein. Bitte erneut aktualisieren.';}
      } finally {
        clearTimeout(timeout);busy=false;controller=null;button?.removeAttribute('aria-busy');
      }
    };
    document.querySelector('[data-status-refresh]').addEventListener('click',event=>{event.preventDefault();refresh();});
    document.addEventListener('visibilitychange',()=>{if(document.hidden){controller?.abort();}else refresh();});
    window.addEventListener('pagehide',()=>{controller?.abort();});
    window.addEventListener('pageshow',()=>{refresh();});
    setInterval(refresh,30000);
    refresh();
  }
  const form=document.querySelector('[data-receipt-form]');
  if(!form) return;
  const inputs=[...form.querySelectorAll('input[type=file]')], note=form.querySelector('[data-file-note]');
  inputs.forEach(input=>input.addEventListener('change',()=>{
    if(!input.files.length) return;
    inputs.filter(other=>other!==input).forEach(other=>{other.value='';other.setCustomValidity('');});
    const file=input.files[0];
    input.setCustomValidity(file.size>8*1024*1024?'Bitte einen Beleg mit höchstens 8 MB auswählen.':'');
    note.textContent=file.name+' · '+Math.ceil(file.size/1024)+' KB';
    input.reportValidity();
  }));
  form.addEventListener('submit',event=>{
    const selected=inputs.filter(input=>input.files.length);
    if(selected.length!==1){event.preventDefault();note.textContent='Bitte einen Beleg fotografieren oder eine Datei auswählen.';return;}
    // Keep the file and request receipt intact so a retry is safe after an error.
    const button=form.querySelector('button[type=submit],button:not([type])');
    button.disabled=true;button.textContent='Beleg wird gespeichert …';
  });
})();
