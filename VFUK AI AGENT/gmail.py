import os
import smtplib
import ssl
from email.message import EmailMessage
from dotenv import load_dotenv

def send_mail(subject: str,
              body: str,
              to=None,
              cc=None,
              bcc=None,
              sender=None,
              html: bool = False) -> None:
    """
    Send a Gmail SMTP email using env configuration.
    Required env:
      - GMAIL_ADDRESS
      - GMAIL_APP_PASSWORD
    Optional env:
      - SMTP_HOST (default: smtp.gmail.com)
      - SMTP_PORT (default: 587)
      - TO_EMAIL (used if `to` is None)
    """
    load_dotenv()
    sender = sender or os.environ["GMAIL_ADDRESS"]
    app_pwd = os.environ["GMAIL_APP_PASSWORD"]
    host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    port = int(os.environ.get("SMTP_PORT", "587"))
    to = to or os.environ.get("TO_EMAIL", sender)

    # normalize recipients
    if isinstance(to, str):
        to = [x.strip() for x in to.split(",") if x.strip()]
    cc = [] if not cc else ([cc] if isinstance(cc, str) else [x.strip() for x in cc if x])
    bcc = [] if not bcc else ([bcc] if isinstance(bcc, str) else [x.strip() for x in bcc if x])

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(to)
    if cc:
        msg["Cc"] = ", ".join(cc)
    if html:
        msg.set_content(body)
        msg.add_alternative(body, subtype="html")
    else:
        msg.set_content(body)

    recipients = to + cc + bcc
    context = ssl.create_default_context()
    with smtplib.SMTP(host, port, timeout=30) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(sender, app_pwd)
        server.send_message(msg, from_addr=sender, to_addrs=recipients)

# Backward-compatible test helper
def send_email():
    load_dotenv()
    subj = "Test Email via Gmail SMTP (python-dotenv)"
    body = "Hello, this was sent securely using STARTTLS and env vars."
    return send_mail(subj, body)

if __name__ == "__main__":
    send_email()
