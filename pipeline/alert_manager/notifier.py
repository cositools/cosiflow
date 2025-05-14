import smtplib
from email.mime.text import MIMEText
from typing import List
import os

def send_notification(message: str, recipients: List[str]):
    smtp_server = os.environ.get('ALERT_SMTP_SERVER', 'mailhog')
    smtp_port = int(os.environ.get('ALERT_SMTP_PORT', '1025'))
    sender_email = os.environ.get('ALERT_EMAIL_SENDER', 'donotreply@cosiflow.alert.errors.it')

    subject = "[CosiFlow ALERT] Error detected"
    body = f"An error was found:\n\n{message}"

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ', '.join(recipients)

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.sendmail(sender_email, recipients, msg.as_string())
            print(f"[EMAIL SENT] {sender_email} -> {recipients}")
    except Exception as e:
        print(f"[EMAIL ERROR] {e}")