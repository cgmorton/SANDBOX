import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

if __name__ == '__main__':
    sender_name = 'climateEngineDeveloper'
    sender_pw = '2018developOnGEE'
    fromaddr = sender_name + "@gmail.com"
    toaddr = "bdaudert@gmail.com"
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "Python email test"
    body = "Hello my dear"
    msg.attach(MIMEText(body, 'plain'))
    text = msg.as_string()

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.ehlo()
    # server.login("climateEngineDeveloper", "2018developOnGEE")
    server.login(sender_name, sender_pw)
    server.sendmail(fromaddr, toaddr, text)
    # server.sendmail("climateEngineDeveloper@gmail.com", "superbritta@gmail.com", msg)
