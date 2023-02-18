"""
sudo systemcrl start sendmail
"""
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import dotenv_values


class MailSender:
    ERROR = 0
    INFO = 1

    def __init__(self):
        self.email_params = dict(dotenv_values(".env.email"))

    def _get_email_server(self):
        """Creates an instance of email server.
    
        Returns:
            server -- SMTP instance
        """

        server = (smtplib.SMTP_SSL if self.email_params.get("ssl", "false") == "true" else smtplib.SMTP)(
            self.email_params.get("server", "localhost"),
            self.email_params.get("port", 25),
        )
        if self.email_params.get("tls", "false") == "true":
            server.starttls()
        if self.email_params.get("auth", "false") == "true":
            server.login(
                self.email_params['username'], self.email_params['password']
            )
        return server

    def send_email(self, type=INFO, text=None, html=None):
        """Send email

        Arguments:
            text {str} -- plain text message
            html {str} -- html message
            server {SMTP} -- SMTP server instance
        """
        mail_to = [self.email_params.get("to", "no-reply@univ-paris1.fr")]
        mail_from = self.email_params.get("from", "no-reply@univ-paris1.fr")
        mail_to_string = ", ".join(mail_to)
        msg = MIMEMultipart("alternative")
        subject = self.email_params.get(
            "error_subject" if type == self.ERROR else "default_subject", "Automatic report email"
        )
        msg["Subject"] = f"{subject} - {datetime.now().date().isoformat()} ({datetime.now().time().isoformat()})"
        msg["From"] = mail_from
        msg["To"] = mail_to_string

        if text is not None:
            part1 = MIMEText(text, "plain")
            msg.attach(part1)

        if html is not None:
            part2 = MIMEText(html, "html")
            msg.attach(part2)

        return self._get_email_server().sendmail(mail_from, mail_to, msg.as_string())
