"""Source-bound receipt drafts. No provider, bank, tax or approval operations.

The optional host hook owns existing AI configuration/transport. Document text
is untrusted data; only this bounded field schema can leave the analysis chain.
"""
from datetime import date
from decimal import Decimal, InvalidOperation
import re
import unicodedata

FIELDS=("lieferant","belegnummer","belegdatum","faelligkeit","betrag_brutto","typ","kategorie")
KINDS={"rechnung","lieferschein","bescheid","post"}
CATEGORIES={"rechnung","gebuehren","steuern","sonstige","privat"}
LABELS={"lieferant":"Lieferant", "belegnummer":"Belegnummer", "belegdatum":"Belegdatum",
        "faelligkeit":"Fälligkeit", "betrag_brutto":"Bruttogesamtbetrag", "typ":"Belegart", "kategorie":"Ordner"}
MAX_TEXT=100000
AI_FIELDS_SCHEMA={"type":"object","additionalProperties":False,
    "properties":{
        "fields":{"type":"object","additionalProperties":False,"required":list(FIELDS),
            "properties":{name:{"type":"object","additionalProperties":False,
                "required":["value","evidence","confidence"],
                "properties":{"value":{"type":"string"},"evidence":{"type":"string","description":"Kurzes wortgetreues Quellzitat mit Feldbezeichnung. Bruttogesamtbetrag benötigt Gesamt-/Bruttobeschriftung; unsicher bedeutet leer."},
                              "confidence":{"type":"string","enum":["hoch","unsicher"]}}} for name in FIELDS}},
        "warnings":{"type":"array","items":{"type":"string"}}},
    "required":["fields","warnings"]}

_DATE=re.compile(r"(?<!\d)(?:\d{4}-\d{2}-\d{2}|\d{1,2}\.\d{1,2}\.\d{4})(?!\d)")
_MONEY=re.compile(r"(?<![\w.,+-])(?:\d{1,3}(?:[. ,\u00a0]\d{3})+|\d+)[.,]\d{2}(?![\d.,])")
_GROSS=re.compile(r"\b(?:bruttobetrag|bruttosumme|rechnungs(?:end)?betrag|gesamtbetrag|gesamtsumme|endbetrag|zahlbetrag|zu\s+zahlen|total\s*(?:gross|amount)|amount\s+due)\b|^\s*brutto\b",re.I)
_INSTRUCTION=re.compile(r"ignore\s+(?:all|previous|prior)|(?:system|developer)\s*(?:prompt|message|instruction)|(?:^|\s)<\/?(?:system|developer)>|ignoriere\s+(?:alle|vorherige|bisherige)",re.I)
_NUMBER_LABEL=re.compile(
    r"^(?:Rechnungs[- ]?(?:nummer|nr\.?)|(?:Neu|Gebraucht)fahrzeugrechnung(?:\s+(?:Nr\.?|Nummer))?"
    r"|Rechnung(?:\s+(?:Nr\.?|Nummer))?|Invoice(?:\s*(?:number|no\.?|#))?"
    r"|Beleg[- ]?(?:nummer|nr\.?))(?:\s*[:#]\s*|\s+)(.*?)\s*$",re.I)
_NUMBER_VALUE=re.compile(r"[\w](?:[\w./-]*[\w])?(?:\s*[-/]\s*[\w](?:[\w./-]*[\w])?)*")
_RECEIPT_RELATIVE=re.compile(r"(?:ab|nach)\s+(?:dem\s+)?(?:Zugang|Erhalt|Eingang)",re.I)


def _normal(value):
    return " ".join(unicodedata.normalize("NFKC",value).casefold().split())


def _text(value):
    if not isinstance(value,str):return ""
    return "".join(c for c in unicodedata.normalize("NFKC",value)[:MAX_TEXT] if ord(c)>=32 or c in "\n\t")


def _day(value):
    if not isinstance(value,str):return ""
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}",value):parsed=date.fromisoformat(value)
        elif re.fullmatch(r"\d{1,2}\.\d{1,2}\.\d{4}",value):
            day,month,year=map(int,value.split("."));parsed=date(year,month,day)
        else:return ""
        return parsed.isoformat() if 1900<=parsed.year<=2100 else ""
    except ValueError:return ""


def _amount(value):
    if not isinstance(value,str) or len(value)>30:return ""
    value=value.strip().replace("\u00a0","").replace(" ","")
    if not re.fullmatch(r"\d[\d.,]*[.,]\d{2}",value):return ""
    if "," in value and "." in value:
        decimal="," if value.rfind(",")>value.rfind(".") else "."
        thousands="." if decimal=="," else ","
        if not re.fullmatch(r"\d{1,3}(?:"+re.escape(thousands)+r"\d{3})+"+re.escape(decimal)+r"\d{2}",value):return ""
        value=value.replace(thousands,"").replace(decimal,".")
    elif value.count(",")>1 or value.count(".")>1:return ""
    else:value=value.replace(",",".")
    try:
        number=Decimal(value)
        return format(number,".2f") if 0<number<Decimal("1000000000") else ""
    except InvalidOperation:return ""


def _value(name,value):
    if not isinstance(value,str) or any(ord(c)<32 for c in value) or _INSTRUCTION.search(value):return ""
    value=" ".join(value.split())
    if name in {"belegdatum","faelligkeit"}:return _day(value)
    if name=="betrag_brutto":return _amount(value)
    if name=="typ":return value if value in KINDS else ""
    if name=="kategorie":return value if value in CATEGORIES else ""
    if name=="belegnummer":
        return value if (0<len(value)<=100 and re.fullmatch(r"[\w][\w./ -]*",value)
                         and any(c.isdigit() for c in value) and not re.search(r"\b(?:IBAN|BIC|SWIFT)\b",value,re.I)) else ""
    if name=="lieferant":
        return value if (0<len(value)<=160 and any(c.isalpha() for c in value)
                         and not re.search(r"\b(?:IBAN|BIC|SWIFT|Bankverbindung)\b",value,re.I)) else ""
    return ""


def _gross_values(evidence):
    """Only explicitly labelled totals, never line prices or a net subtotal."""
    found=[]
    for line in evidence.splitlines():
        gross=_GROSS.search(line)
        if (not gross or re.search(r"\b(?:USD|CHF|GBP|JPY|Einzelpreis|Stückpreis|Positionspreis|pro\s+Stück|je\s+Stück)\b",line,re.I)):
            continue
        tail=line[gross.end():]
        later=list(re.finditer(r"\bbrutto(?:betrag|summe)?\b",tail,re.I))
        if later:tail=tail[later[-1].end():]
        elif re.search(r"\bnetto\b",line,re.I) and not re.search(r"brutto",gross[0],re.I):continue
        # OCR can join the currency column to the labelled total. This only
        # inserts a separator; it never calculates or reinterprets an amount.
        tail=re.sub(r"\bEUR(?=\d)","EUR ",tail,flags=re.I)
        for item in _MONEY.finditer(tail):
            if re.search(r"[-–−]\s*$",tail[:item.start()]):continue
            amount=_amount(item[0])
            if amount:found.append(amount)
    return found


def _classify(text):
    # Lazy import avoids the bridge/module cycle while retaining its rules.
    from werkstatt_buero_lexware import classify_scan
    # These explicit invoice headings have the same meaning as Rechnung.
    # Preserve the original text in field evidence rather than rewriting it.
    normalised=re.sub(r"(?im)^(\s*)(?:Neu|Gebraucht)fahrzeugrechnung\b",r"\1Rechnung",text)
    result=classify_scan(normalised)
    if (not result["blocked_auto"] and not result["suggested_kind"]
            and re.search(r"(?im)^\s*Bescheid(?:\s+[üu]ber\b|\s*:|\s*$)",text[:4000])):
        category=("steuern" if re.search(r"steuer",text,re.I) else
                  "gebuehren" if re.search(r"geb[üu]hr|beitrag|abgabe",text,re.I) else "sonstige")
        result.update(category=category,suggested_kind="bescheid",
                      hint="Bescheid erkannt. Betrieblicher Bezug und Zahlungspflicht müssen ausdrücklich geprüft werden.")
    return result


def _number_from_line(line):
    match=_NUMBER_LABEL.fullmatch(line.strip())
    if not match:return ""
    value=match[1].strip()
    # Consume the whole identifier. A prefix before a spaced dash or trailing
    # unrelated text is never treated as a complete invoice number.
    return _value("belegnummer",value) if _NUMBER_VALUE.fullmatch(value) else ""


def local_fields(text):
    text=_text(text);lines=[line.strip() for line in text.splitlines() if line.strip()]
    candidates={name:[] for name in FIELDS};warnings=[]
    def add(name,value,quote):
        value=_value(name,value)
        if value:candidates[name].append((value,quote[:500]))
    for index,line in enumerate(lines):
        if _INSTRUCTION.search(line):
            warnings.append("Dokument enthält fremde Modellanweisungen; diese werden nicht als Arbeitsauftrag ausgeführt.");continue
        match=re.match(r"(?:Lieferant|Rechnungssteller|Aussteller|Absender)\s*[:]\s*(.+)$",line,re.I)
        if match:add("lieferant",match[1],line)
        number=_number_from_line(line)
        if number:add("belegnummer",number,line)
        # A lone header Datum is usable; Leistungs-/Lieferdatum and a mixed
        # multi-date line are not silently reinterpreted as the document date.
        if index<30:
            generic=re.fullmatch(r"Datum\s*:\s*(\d{4}-\d{2}-\d{2}|\d{1,2}\.\d{1,2}\.\d{4})",line,re.I)
            if generic:add("belegdatum",generic[1],line)
        for name,pattern in (("belegdatum",r"(?:Rechnungsdatum|Belegdatum|Ausstellungsdatum|Invoice\s+date)\b"),
                             ("faelligkeit",r"(?:F[äa]llig(?:keit)?(?:\s+am)?|zahlbar\s+bis|Zahlungsziel(?:\s+bis)?|Due\s+date)\b")):
            if re.match(pattern,line,re.I):
                if name=="faelligkeit" and _RECEIPT_RELATIVE.search(line):continue
                for found in _DATE.finditer(line):add(name,found[0],line)
        gross=_GROSS.search(line)
        if gross:
            if re.search(r"\b(?:USD|CHF|GBP|JPY)\b",line,re.I):
                warnings.append("Fremdwährungsbetrag erkannt; ohne Währungsumrechnung nicht vorbelegt.");continue
            for found in _gross_values(line):add("betrag_brutto",found,line)
    if not candidates["lieferant"]:
        header=[]
        for line in lines[:12]:
            if re.match(r"(?:Rechnung(?:\b|s)|Invoice\b|Rechnungsempf[äa]nger\b|Empf[äa]nger\b|An\s*[:])",line,re.I):break
            if not _INSTRUCTION.search(line) and re.search(r"\b(?:GmbH(?:\s*&\s*Co\.?\s*KG)?|AG|UG(?:\s*\(haftungsbeschr[äa]nkt\))?|e\.?\s*K\.?|OHG|KG)\s*$",line,re.I):
                header.append((line,line))
        for value,quote in header:add("lieferant",value,quote)
    classification=_classify(text)
    evidence="\n".join(lines[:12])[:500]
    if classification["suggested_kind"]:add("typ",classification["suggested_kind"],evidence)
    if classification["suggested_kind"] or classification["category"]!="sonstige":add("kategorie",classification["category"],evidence)
    fields,origins,ambiguous={},{},set()
    for name,values in candidates.items():
        unique={value for value,_ in values}
        if len(unique)==1:
            fields[name]=values[0][0];origins[name]={"source":"local","evidence":values[0][1]}
        elif len(unique)>1:
            ambiguous.add(name)
            warnings.append(LABELS[name]+": mehrere abweichende Quellwerte; nicht vorbelegt.")
    return fields,origins,list(dict.fromkeys(warnings)),classification,ambiguous


def _validate_payload(data):
    if not isinstance(data,dict) or set(data)!={"fields","warnings"}:raise ValueError()
    fields,warnings=data["fields"],data["warnings"]
    if (not isinstance(fields,dict) or set(fields)!=set(FIELDS) or not isinstance(warnings,list)
            or len(warnings)>20 or any(not isinstance(v,str) or len(v)>500 for v in warnings)):raise ValueError()
    for item in fields.values():
        if (not isinstance(item,dict) or set(item)!={"value","evidence","confidence"}
                or not all(isinstance(v,str) for v in item.values()) or len(item["value"])>200
                or len(item["evidence"])>500 or item["confidence"] not in {"hoch","unsicher"}):raise ValueError()
    return fields


def _grounded(name,value,evidence):
    if name in {"belegdatum","faelligkeit"}:
        dates={_day(m[0]) for m in _DATE.finditer(evidence)}-{''}
        if dates!={value}:return False
        if name=="belegdatum":
            return not (re.search(r"Leistungsdatum|Lieferdatum",evidence,re.I)
                and not re.search(r"Rechnungsdatum|Belegdatum|Ausstellungsdatum|Invoice\s+date|Datum\s+des\s+Belegs",evidence,re.I))
        return (not _RECEIPT_RELATIVE.search(evidence)
                and bool(re.search(r"F[äa]llig(?:keit)?|zahlbar\s+bis|Zahlungsziel|Due\s+date|bis\s+zum",evidence,re.I)))
    if name=="betrag_brutto":
        return value in _gross_values(evidence)
    if name=="typ":return _classify(evidence)["suggested_kind"]==value
    if name=="kategorie":return _classify(evidence)["category"]==value
    if name=="belegnummer":
        return any(_normal(value)==_normal(_number_from_line(line)) for line in evidence.splitlines())
    if name=="lieferant" and re.search(r"rechnungsempf[äa]nger|empf[äa]nger|bill\s+to",evidence,re.I) and not re.search(r"lieferant|aussteller|rechnungssteller|supplier",evidence,re.I):
        return False
    return _normal(value) in _normal(evidence)


def analyse_document(text,*,ai_hook=None,ai_enabled=False,path=None,filename=""):
    """Return validated draft fields plus origin/status; never mark verified.

    Host hook: text/schema/path/filename -> {data:<schema>,visual_evidence:bool}.
    visual_evidence is host transport metadata, never a generated model field.
    Only an explicitly enabled, injected hook may use existing online services.
    """
    text=_text(text)
    fields,origins,warnings,classification,ambiguous=local_fields(text)
    local_keys=set(fields);ai_keys=set();status="local"
    if not text:warnings.append("Keine verlässliche lokale Texterkennung; fehlende Werte am Original prüfen.")
    if ai_enabled and not callable(ai_hook):
        status="local_no_ai";warnings.append("KI-Analyse ist nicht angebunden; ausschließlich lokale Erkennung.")
    elif ai_enabled:
        try:
            response=ai_hook(text=text,schema=AI_FIELDS_SCHEMA,path=path,filename=filename)
            if isinstance(response,dict) and response.get("unavailable") is True:
                status="local_ai_unavailable";warnings.append("Vorhandene KI-Konfiguration ist nicht verfügbar; ausschließlich lokale Erkennung.")
            else:
                if not isinstance(response,dict) or set(response)-{"data","visual_evidence"}:raise ValueError()
                visual=response.get("visual_evidence") is True
                ai=_validate_payload(response.get("data"))
                for name,item in ai.items():
                    if not item["value"]:continue
                    if name in ambiguous:
                        warnings.append(LABELS[name]+": mehrdeutige Quellwerte bleiben auch bei einem KI-Vorschlag leer; persönlich prüfen.")
                        continue
                    value=_value(name,item["value"]);evidence=_text(item["evidence"])
                    if (not value or item["confidence"]!="hoch" or not evidence or _INSTRUCTION.search(evidence)
                            or not _grounded(name,value,evidence) or not visual and _normal(evidence) not in _normal(text)):
                        warnings.append(LABELS[name]+": KI-Wert unsicher oder nicht ausreichend an der Quelle belegt; nicht übernommen.");continue
                    if name in fields and fields[name]!=value:
                        fields.pop(name);origins.pop(name,None);local_keys.discard(name)
                        ambiguous.add(name)
                        warnings.append(LABELS[name]+": lokale Erkennung und KI widersprechen sich; am Original prüfen.");continue
                    if name not in fields:
                        fields[name]=value;origins[name]={"source":"ai_visual" if visual else "ai","evidence":evidence}
                        ai_keys.add(name)
                status="ai_assisted" if ai_keys else "local_ai_unconfirmed"
                if visual and ai_keys:warnings.append("Bildgestützte KI-Vorschläge sind nicht durch OCR bestätigt; Original vor Freigabe prüfen.")
                if response["data"]["warnings"]:warnings.append("KI meldet zusätzliche Unsicherheiten; Original prüfen.")
                if not ai_keys:warnings.append("KI-Antwort liefert keine zusätzlich ausreichend belegten Werte.")
        except Exception:
            status="local_ai_failed";warnings.append("KI-Analyse fehlgeschlagen oder Antwort ungültig; lokale Erkennung bleibt als Vorschlag erhalten.")
    missing=[LABELS[name] for name in FIELDS if name not in fields]
    if missing:warnings.append("Noch offen: "+", ".join(missing)+".")
    warnings=list(dict.fromkeys(warnings))
    source="mixed" if ai_keys and local_keys else "ai" if ai_keys else "local"
    classification=dict(classification)
    if "typ" in fields:classification["suggested_kind"]=fields["typ"]
    if "kategorie" in fields:classification["category"]=fields["kategorie"]
    # Accepted image evidence can identify private tax/other noninvoice post
    # even when OCR is empty. It remains a proposal, never a verified identity.
    safety=_classify("\n".join([text,*(item["evidence"] for item in origins.values())]))
    if safety["blocked_auto"]:classification.update(safety)
    origin_labels={"local":"lokal","ai":"KI mit Quelltext","ai_visual":"KI-Bild, nicht OCR-bestätigt"}
    origin_hint="Feldherkunft: "+"; ".join(LABELS[name]+": "+origin_labels[item["source"]] for name,item in origins.items())+"." if origins else ""
    hint=" ".join([classification["hint"],"Erkennung: "+source+". Alle Werte sind prüfpflichtige Vorschläge.",origin_hint,*warnings])[:1000]
    return {**classification,"fields":fields,"field_sources":origins,"warnings":warnings,
            "analysis_status":status,"source":source,"hint":hint,"identity_verified":False,
            "ambiguous_fields":sorted(ambiguous)}
