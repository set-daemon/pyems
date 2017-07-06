#!/usr/bin/env python
#-*- encoding: utf-8 -*-

'''
	purpose: 实现消息客户端，收发、解码消息，暂时只支持redis
	author : set_daemonr@126.com
	date   : 2017-06-21
	history: 
'''

import redis
import json

from msg_resolve import MsgResolve

class MsgClient(object):
	'''
	这里的消息通道是单通道的
	'''
	class Scheme(object):
		def __init__(self, scheme_data):
			self.scheme = scheme_data
			segs = scheme_data.split(":")
			self.db_type = segs[0]	
			self.host = segs[1].lstrip("//")
			self.port = int(segs[2])
			self.db = segs[3]

	def __init__(self, **kw):
		self.config = {}

		for k in kw.keys():
			self.config[k] = kw[k]

		self.scheme = MsgClient.Scheme(self.config["scheme"])
		self.chn_name = self.config["channel_name"]
		self.reconnect()

	def reconnect(self):
		if self.scheme.db_type == "redis":
			self.cli = redis.Redis(host = self.scheme.host, port = self.scheme.port, db = self.scheme.db)

	def get_msgs(self, num):
		'''
		读取指定数量的消息，如果num <= 0，则取出最大255条消息
		'''
		msgs = []
		if num <= 0:
			num = 255
		for i in range(0, num):
			msg_data = self.cli.lpop(self.chn_name)
			if msg_data is None:
				break
			msg = MsgResolve.parse(msg_data)
			if msg is None:
				continue
			msgs.append(msg)

		return msgs

	def send_msg(self, msg):
		'''
		发送消息
		'''
		self.cli.rpush(self.chn_name, msg)

		return True

if __name__ == "__main__":
	config = {
		"scheme": "redis://127.0.0.1:6379:3",
		"channel_name": "test_channel"
	}
	cli = MsgClient(config)

	msg = {
		"msg_type": "show",
		"from": "test",
		"to": "test",
		"msg_data": {
			"result": "ok"
		}
	}
	cli.send_msg(json.dumps(msg))
	msg["from"] = "test2"
	cli.send_msg(json.dumps(msg))
	msg["from"] = "test3"
	cli.send_msg(json.dumps(msg))

	msgs = cli.get_msgs(-1)
	for msg in msgs:
		print msg.to_str()
