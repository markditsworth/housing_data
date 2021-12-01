import smtplib
import os
from config.base import *

def sendEmail(body):
    subject = "Housing Scrape Notification"
    text = """
    From: %s
    To: %s
    Subject: %s

    %s
    """%(gmail_address, notification_address, subject, body)
    try:
        server_ssl = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server_ssl.ehlo()
        server_ssl.login(gmail_address, gmail_password)
        server_ssl.sendmail(gmail_address, notification_address, text)
        server_ssl.close()
        return False
    except Exception as e:
        return str(e)
    
