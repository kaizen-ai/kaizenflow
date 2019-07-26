import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_email(
        subject,
        message,
        to_adr,
        email_address=None,
        email_password=None,
        html=False,
):
    """
    Send mail to specified e-mail addresses
    :param message: Message to be sent
    :param to_adr: Mail to which to send messages
    :type list
    :return: None
    """
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    if email_address is None:
        email_address = os.environ["EMAIL_ADDRESS"]
    if email_password is None:
        email_password = os.environ["EMAIL_PASSWORD"]
    server.login(email_address, email_password)
    msg = MIMEMultipart()
    msg['From'] = email_address
    msg['To'] = ", ".join(to_adr)
    msg['Subject'] = subject
    if html:
        msg.attach(MIMEText(message, 'html'))
    else:
        msg.attach(MIMEText(message, 'plain'))

    text = msg.as_string()
    server.sendmail(email_address, to_adr, text)