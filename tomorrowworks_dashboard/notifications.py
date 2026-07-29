from __future__ import annotations

import smtplib
import ssl
from email.message import EmailMessage


def mail_konfiguriert(config) -> bool:
    return bool(config.get("SMTP_HOST") and config.get("SMTP_FROM"))


def mail_senden(config, empfaenger: str, betreff: str, text: str) -> tuple[str, str]:
    if not empfaenger.strip():
        return "keine_adresse", "Keine Empfängeradresse hinterlegt."
    if not mail_konfiguriert(config):
        return "nicht_konfiguriert", "SMTP-Mailversand ist noch nicht eingerichtet."

    nachricht = EmailMessage()
    nachricht["Subject"] = betreff
    nachricht["From"] = config["SMTP_FROM"]
    nachricht["To"] = empfaenger
    nachricht.set_content(text)

    try:
        port = int(config.get("SMTP_PORT", 587))
        timeout = int(config.get("SMTP_TIMEOUT", 12))
        if config.get("SMTP_SSL"):
            server = smtplib.SMTP_SSL(
                config["SMTP_HOST"],
                port,
                timeout=timeout,
                context=ssl.create_default_context(),
            )
        else:
            server = smtplib.SMTP(config["SMTP_HOST"], port, timeout=timeout)
        with server:
            if config.get("SMTP_TLS") and not config.get("SMTP_SSL"):
                server.starttls(context=ssl.create_default_context())
            if config.get("SMTP_USER"):
                server.login(config["SMTP_USER"], config.get("SMTP_PASSWORD", ""))
            server.send_message(nachricht)
    except (OSError, smtplib.SMTPException) as exc:
        return "fehlgeschlagen", str(exc)[:500]
    return "gesendet", ""
