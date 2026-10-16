(() => {
  'use strict';
  const search=document.querySelector('[data-fleet-search]');
  if(search) search.addEventListener('input',()=>{
    let visible=0;
    document.querySelectorAll('[data-fleet-card]').forEach(card=>{
      card.hidden=!card.dataset.search.includes(search.value.trim().toLowerCase());
      if(!card.hidden) visible++;
    });
    document.querySelector('[data-search-empty]').hidden=visible>0;
  });
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
