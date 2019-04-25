import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

EMAIL_ADDRESS = ''
EMAIL_PASSWORD = ''


def send_email(subject,
               message,
               to_adr,
               email_address=EMAIL_ADDRESS,
               email_pass=EMAIL_PASSWORD,
               html=False,
               ):
    """
    Send mail to specified e - mail addresses
    :param message: Message to be sent
    :param to_adr: Mail to which to send messages
    :type list
    :return: None
    """
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(email_address, email_pass)
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
