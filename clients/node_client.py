#!/usr/bin/env python
#-*- encoding: utf-8 -*-

'''
	purpose: 实现节点信息管理客户端，暂时只支持redis
	author : set_daemonr@126.com
	date   : 2017-06-25
	history: 
'''

import redis
import json

import socket
import uuid
import os

class NodeInfo(object):
	def __init__(self, **kw):
		self.info = {}
		for k in kw.keys():
			v = kw[k]
			self.info[k] = v

	@staticmethod
	def get_mac():
		mac = uuid.UUID(int = uuid.getnode()).hex[-12:]
		return ":".join([mac[e:e+2] for e in range(0,11,2)])

	def create(self, **kw):
		for k in kw.keys():
			self.info[k] = kw[k]	
		self.info["mac"] = NodeInfo.get_mac()
		self.info["hostName"] = socket.getfqdn(socket.gethostname())
		self.info["ip"] = socket.gethostbyname(self.info["hostName"])
		self.info["pid"] = os.getpid()

	def set(self, **kw):
		for k in kw.keys():
			self.info[k] = kw[k]

class NodeClient(object):
	'''
	这里的消息通道是单通道的
	'''
	class Scheme(object):
		def __init__(self, scheme_data):
			segs = scheme_data.split(":")
			self.db_type = segs[0]	
			self.host = segs[1].lstrip("//")
			self.port = int(segs[2])
			self.db = segs[3]

	def __init__(self, config):
		self.config = config

		self.scheme = NodeClient.Scheme(self.config["scheme"])
		self.chn_name = self.config["channel_name"]
		self.reconnect()

	def reconnect(self):
		if self.scheme.db_type == "redis":
			self.cli = redis.Redis(host = self.scheme.host, port = self.scheme.port, db = self.scheme.db)

	def offline(self, node_info):
		key = "node:%s:%s" %(node_info.info["nodeType"],
			node_info.info["nodeId"])
		self.cli.hmset(key, {"status":0})

	def destroy(self, node_info):
		key = "node:%s:%s" %(node_info.info["nodeType"],
			node_info.info["nodeId"])

		self.cli.delete(key)

	def get_nodes(self, node_type):
		key = "node:%s*" %(node_type)
		node_list = self.cli.keys(key)
		# 去除前缀
		for i in range(0, len(node_list)):
			node_list[i] = node_list[i].lstrip('node:')

		return node_list

	def get_node_info(self, node_id):
		# node_id 包含node类型
		key = "node:%s" %(node_id)
		info = self.cli.hgetall(key)	

		return NodeInfo(**info)

	def sync_node(self, node):
		'''
		同步节点信息
		'''
		key = "node:%s:%s" %(node.info["nodeType"], node.info["nodeId"])
		self.cli.hmset(key, node.info)

if __name__ == "__main__":
	#config = {
	#	"scheme": "redis://127.0.0.1:6379:3",
	#	"channel_name": "test_channel"
	#}
	from config import global_config
	cli = NodeClient(global_config["node_config"])

	node_info = NodeInfo(**{"nodeType":"worker", "nodeId":"134", "pid":"1244", "ip":"3442332" })
	cli.sync_node(node_info)
