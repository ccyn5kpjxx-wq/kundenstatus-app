(() => {
  'use strict';
  const buttons=Array.from(document.querySelectorAll('[data-time-action]'));
  if(!buttons.length)return;
  const status=document.getElementById('personal-status');let pending=false;
  buttons.forEach(button=>button.addEventListener('click',async()=>{
    if(pending)return;
    const host=window.AssistantWorkflowHost;
    if(!host){status.textContent='Assistent noch nicht bereit. Bitte die Seite erneut öffnen.';return;}
    pending=true;buttons.forEach(item=>{item.disabled=true;});host.stopVoice();
    try{
      await host.api('/vorschlag',{art:'arbeitszeit',aktion:button.dataset.timeAction});
      await host.proposed();status.textContent='Zeitstempel vorbereitet. Bitte den Vorschlag prüfen und bestätigen.';
    }catch(error){status.textContent=error.message||'Zeitstempel konnte nicht vorbereitet werden.';}
    finally{pending=false;buttons.forEach(item=>{item.disabled=false;});}
  }));
})();
