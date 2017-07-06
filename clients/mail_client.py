#!/usr/bin/env python
#-*- encoding: utf-8 -*-

'''
	purpose: 实现邮件smtp客户端
	author : set_daemonr@126.com
	date   : 2017-06-21
	history: 
'''
import sys
import os

import smtplib
import mimetypes
import email
from email.utils import COMMASPACE
from email.utils import parseaddr, formataddr
from email.header import Header
from email.Encoders import encode_base64

class MailClient(object):
	class Scheme(object):
		def __init__(self, scheme_data):
			'''
			scheme_data格式；
				ssl://smtp_host/port?user=&password=&aliasName=
				non_ssl://smtp_host/port?user=&password=&aliasName=
			'''
			self.scheme_data = scheme_data
			self.parse(self.scheme_data)

		def parse(self, scheme_data):
			if scheme_data is not None and scheme_data != "":
				self.scheme_data = scheme_data

			segs = self.scheme_data.split(':')
			# TODO check
			if segs[0] == "ssl":
				self.ssl = True
			else:
				self.ssl = False
			other_data = segs[1].lstrip('//')
			segs = other_data.split('?')
			s_segs = segs[0].split('/')
			self.host = s_segs[0]
			self.port = int(s_segs[1])

			s_segs = segs[1].split('&')
			for seg in s_segs:
				sub_seg = seg.split('=')
				k = sub_seg[0]
				v = ""
				if len(sub_seg) > 1:
					v = sub_seg[1]
				if k == 'user':
					self.user = v
				elif k == 'password':
					self.password = v
				elif k == 'aliasName':
					self.alias_name = v
				else:
					pass

	def __init__(self, scheme):
		self.scheme = MailClient.Scheme(scheme)

	def __format_addr(self, s):
		name, addr = parseaddr(s)
		return formataddr((\
			Header(name, 'utf-8').encode(), \
			addr.encode('utf-8') if isinstance(addr, unicode) else addr))

	def connect(self):
		if self.scheme.ssl:
			self.cli = smtplib.SMTP_SSL(self.scheme.host, port = self.scheme.port)
			self.cli.ehlo()
			#self.cli.starttls()
			self.cli.ehlo()
		else:
			self.cli = smtplib.SMTP(self.scheme.host, port = self.scheme.port)

		return True

	def login(self):
		try:
			self.cli.login(self.scheme.user, self.scheme.password)
		except smtplib.SMTPAuthenticationError, e:
			return -1

		return 0

	def logout(self):
		self.cli.close()

	def send_mail(self, recipients, msg):
		'''
			recipients 是接收者列表
			msg 是由MIMEMultipart执行as_string得到的数据
		'''
		msg_obj = email.message_from_string(msg)
		del msg_obj["From"]
		del msg_obj["To"]
		#print self.scheme.alias_name
		msg_obj["From"] = self.__format_addr(u'%s <%s>' %(unicode(self.scheme.alias_name, "utf-8"), self.scheme.user))
		msg_obj["To"] = COMMASPACE.join(recipients)

		ret = -1
		reason = ""
		try:
			result = self.cli.sendmail(self.scheme.user, recipients, msg_obj.as_string())
			if len(result) <= 0:
				ret = 0
			else:
				ret = -1
		except smtplib.SMTPRecipientsRefused, e:
			ret = -2
			for r in e.recipients.keys():
				d = e.recipients[r]
				reason = "%s; %s:%s" %(reason, r, d[1])
		except smtplib.SMTPDataError, e:
			ret = -3
			reason = e[1]
		except smtplib.SMTPServerDisconnected, e:
			ret = -4
			reason = e[0]
		except smtplib.SMTPSenderRefused, e:
			ret = -5
			reason = e[1]

		return (ret, reason)
