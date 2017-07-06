#!/usr/bin/env python
#-*- encoding: utf-8 -*-
import sys
import os

import json

import mimetypes
import email
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase  
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage
from email.MIMEAudio import MIMEAudio  

from email import encoders
from email.utils import parseaddr, formataddr
from email.header import Header
from email.Encoders import encode_base64
from email.utils import COMMASPACE

import smtplib

class MailCreator(object):
	def __init__(self):
		pass	

	def __format_addr(self, s):
		name, addr = parseaddr(s)
		return formataddr((\
			Header(name, 'utf-8').encode(), \
			addr.encode('utf-8') if isinstance(addr, unicode) else addr))

	def load(self, mail_file_name):
		'''
		从文件中读入并生成邮件头和体
		{
			"from": "",
			"aliasName": "",
			"to": [],
			"cc": [],
			"bcc": [],
			"subject": "",
			"content": "",
			"embedRes": [
				{
					"resName": "",
					"file": "",
					"type": ""
				}
			],
			"attachments": [
				{
					"type": "",
					"file": ""
				}
			]
		}
		'''	
		mail_obj = json.loads(open(mail_file_name, "r").read())

		self.msg = MIMEMultipart(_subtype="related")
		sender = ""
		sender_alias = ""
		if "from" in mail_obj:
			sender = mail_obj
		if "aliasName" in mail_obj:
			sender_alias = mail_obj["aliasName"]	
		if sender != "":
			self.msg["From"] = self.__format_addr(u'%s <%s>' %(sender_alias, sender))

		if "to" in mail_obj and len(mail_obj["to"]) > 0: 
			self.msg["To"] = COMMASPACE.join(mail_obj["to"])

		if "cc" in mail_obj and len(mail_obj["cc"]) > 0:
			self.msg["Cc"] = COMMASPACE.join(mail_obj["cc"])

		if "bcc" in mail_obj and len(mail_obj["bcc"]) > 0:
			self.msg["Bcc"] = COMMASPACE.join(mail_obj["bcc"])

		self.msg["Subject"] = Header(mail_obj["subject"], 'utf-8').encode()
		self.msg.attach(MIMEText(mail_obj["content"], 'html', 'utf-8'))

		if len(mail_obj["embedRes"]) > 0:
			for res in mail_obj["embedRes"]:
				with open(res["file"], 'rb') as f:
					if res["type"] == "img":
						img = MIMEImage(f.read())
						img.add_header('Content-ID', res["resName"])
						self.msg.attach(img)
					else:
						pass

		if len(mail_obj["attachments"]) > 0:
			for res in mail_obj["attachments"]:
				res_file = os.path.basename(res["file"]).encode('gbk')	
				with open(res["file"], 'rb') as f:
					doc = MIMEText(f.read(), "base64", "gb2312")
					doc["Content-Type"] = "application/octet-stream"
					doc["Content-Disposition"] = 'attachment; filename="' + res_file +'"'
					self.msg.attach(doc)

	def update_sender(self, sender, alias_name):
		del self.msg["From"]
		self.msg["From"] = 	self.__format_addr(u'%s <%s>' %(alias_name, sender))

	def update_recv(self, recv):
		del self.msg["To"]
		if isinstance(recv, basestring):
			self.msg["To"] = recv
		elif isinstance(recv, list):
			self.msg["To"] = COMMASPACE.join(recv)

	def debug(self):
		print self.msg.as_string()

	def to_msg(self):
		return self.msg.as_string()

if __name__ == "__main__":
	mail_creator = MailCreator()
	mail_creator.load(sys.argv[1])
	#mail_creator.debug()

	msg_str = mail_creator.to_msg()
	msg = email.message_from_string(msg_str)
	del msg['From']
	print msg.get('From')
	del msg['To']
	print msg.get('To')

	sys.exit(0)

	cli = smtplib.SMTP('smtp.126.com', 25)
	cli.login('xxxxx@126.com', 'xxxx')
	cli.sendmail("xxxxx@126.com", ["xxxx@qq.com"], mail_creator.to_msg())

