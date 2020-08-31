import os
import smtplib
import email.mime.multipart as mp
import email.mime.text as mt


def send_email(
    subject: str,
    message: str,
    to_adr: str,
    email_address: str = None,
    email_password: str = None,
    html: str = False,
):
    """Send mail to specified e-mail addresses.

    :param message: Message to be sent
    :param to_adr: Mail to which to send messages
    :type list
    :return: None
    """
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    if email_address is None:
        email_address = os.environ["EMAIL_ADDRESS"]
    if email_password is None:
        email_password = os.environ["EMAIL_PASSWORD"]
    server.login(email_address, email_password)
    msg = mp.MIMEMultipart()
    msg["From"] = email_address
    msg["To"] = ", ".join(to_adr)
    msg["Subject"] = subject
    if html:
        msg.attach(mt.MIMEText(message, "html"))
    else:
        msg.attach(mt.MIMEText(message, "plain"))

    text = msg.as_string()
    server.sendmail(email_address, to_adr, text)
