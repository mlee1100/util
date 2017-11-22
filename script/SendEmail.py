import smtplib
import os
import sys
import contextlib
import unicodecsv as csv
import email
import re
import datetime
import MySQLdb as mdb
import MySQLdb.cursors
import ntpath
from email.mime.text import MIMEText
import time
import requests
import argparse

reload(sys)
sys.setdefaultencoding('UTF8')


parser = argparse.ArgumentParser(description='basic email sending script that uses mailgun api')
parser.add_argument('-s','--subject',required=True, type=str, default='michael.lee@netwisedata.com', help='email subject line')
parser.add_argument('-t','--toaddress',required=False, default='michael.lee@netwisedata.com', type=str, help='email recipients')
parser.add_argument('-f','--fromaddress',required=False, default='notifier@netwisedata.com', type=str, help='email from field')
parser.add_argument('-b','--body',required=False, default = ' ', type=str, help='email body')

args = parser.parse_args()

mailgun_api_key = os.getenv('MAILGUN_API_KEY',None)
mailgun_api_url = os.getenv('MAILGUN_API_URL',None)


def send_mailgun_email(**kwargs):
	email_subject = kwargs.get('email_subject')
	email_from = kwargs.get('email_from')
	email_to = kwargs.get('email_to')
	email_body = kwargs.get('email_body')

	r = requests.post(
		mailgun_api_url,
		auth=("api", mailgun_api_key),
		data={
			"from": email_from,
			"to": email_to,
			"subject": email_subject,
			"text": email_body,
			}
		)

	if int(r.status_code) == 200:
		print 'Successfully sent {subject}'.format(subject=email_subject)
	else:
		print 'there was some error sending the email'
		print r
		print r.content


def main():

	try:
		send_mailgun_email(email_subject=args.subject,email_from=args.fromaddress,email_to=args.toaddress,email_body=args.body)
	except Exception as e:
		print 'failed to send email'
		print e
		sys.exit(1)


if __name__ == '__main__':

	main()



