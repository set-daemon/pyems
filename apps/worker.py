#!/usr/bin/env python
#-*- encoding: utf-8 -*-

'''
	purpose: 实现邮件smtp客户工作端
	author : set_daemonr@126.com
	date   : 2017-06-21
	history: 
'''

import sys
file_dir = sys.path[0]
sys.path.insert(0, file_dir + "/./")
sys.path.insert(0, file_dir + "/../")
sys.path.insert(0, file_dir + "/../clients")
sys.path.insert(0, file_dir + "/../common")

import threading
import time
import datetime
import signal
import os
import uuid
import json
import base64

import logging

from config import global_config
from msg_client import MsgClient
from mail_client import MailClient
from mail_account_client import MailAccountClient
from job_client import JobClient
from task_client import TaskClient
from node_client import NodeClient,NodeInfo
from proto import msg_pb2

g_exist = True
class Worker(threading.Thread):
	def __del__(self):
		self.node_client.offline(self.node_info)

	def __init__(self, config):
		super(Worker, self).__init__()

		self.running = True

		self.config = config

		self.start_time = datetime.datetime.now()

		# 节点管理信息
		self.node_client = NodeClient(self.config["node_config"])
		# 查找如果已经存在相同nodeId的worker，则报错退出，注意：当多个同时启动时会出现异常），注意：nodeId是由调用者生成的，存放在config中！！！
		q_node_info = self.node_client.get_node_info("worker:%s" %(self.config["worker"]["nodeId"]))
		if q_node_info is not None and len(q_node_info.info) > 0 and "nodeId" in q_node_info.info and q_node_info.info["status"] == "1":
			err_msg =  "相同节点的id %s已经启动" %(self.config["worker"]["nodeId"])
			print err_msg
			raise Exception(err_msg)

		self.node_info = NodeInfo()
		self.node_info.create(**{
			"nodeType": "worker",
			"nodeId": self.config["worker"]["nodeId"],
			"instId": uuid.uuid1(),
			"msgScheme": self.config["shell_config"]["scheme"],
			"status": 1,
			"taskId": "",
			"free": "1"
		})
		self.node_info.set(**{
			"channel": "msgchn:%s:%s" %(self.node_info.info["nodeType"]
					  ,self.node_info.info["nodeId"]),
			"logTime": datetime.datetime.strftime(self.start_time, "%Y-%m-%d %H:%M:%S")
		})

		self.node_client.sync_node(self.node_info)

		# 节点消息通道
		self.msg_cli = MsgClient(**{
			"scheme": self.node_info.info["msgScheme"],
				"channel_name": self.node_info.info["channel"],
		})

		# Scheduler消息通道
		node_keys = self.node_client.get_nodes("scheduler")
		scheduler_node_info = None
		for key in node_keys:
			key = key.lstrip('node:')
			scheduler_node_info = self.node_client.get_node_info(key)
			if scheduler_node_info.info["status"] == "1":
				break
		self.sch_msg_cli = MsgClient(**{
			"scheme": scheduler_node_info.info["msgScheme"],
			"channel_name": scheduler_node_info.info["channel"]
		})

		self.mail_client = None

		# task管理客户端
		self.task_cli = TaskClient(self.config["task_config"])

		# job管理客户端
		self.job_cli = JobClient(self.config["job_config"])

		# 邮件帐号管理客户端
		self.mail_account_client = MailAccountClient(self.config["mails_config"])

		logging.basicConfig(level=logging.INFO,
			format="%(asctime)s %(filename)s%(levelname)s %(message)s",
			datefmt='%a, %d %b %Y %H:%M:%S',
			filename="%s/%s_%s_%s.log" %(self.config["worker"]["logpath"],
					self.node_info.info["nodeType"],
					self.node_info.info["nodeId"],
					datetime.datetime.strftime(self.start_time, "%Y%m%d%H")),
			filemode='a')

	def process_msg(self, msg):
		'''
		处理收到的msg
		'''
		# TODO 此处应该设计成 函数映射
		logging.info('worker|%s|%d|to_process|%s|%s' %(self.node_info.info["nodeId"],
				msg.header.type, msg.header.source, msg.header.dest))
		if msg.header.type == msg_pb2.WORKER_OFF_REQ:
			self.running = False
		if msg.header.type == msg_pb2.MAIL_ALTER_REQ:
			# 下线当前工作的mail
			if self.mail_client is not None:
				self.mail_client.logout()
				self.mail_client = None
			mail_scheme = self.mail_account_client.get_account_scheme(msg.account)
			self.mail_client = MailClient(mail_scheme)

			rsp = msg_pb2.MailAlterRsp()
			rsp.header.type = msg_pb2.MAIL_ALTER_RSP
			rsp.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			rsp.header.dest = msg.header.source
			rsp.header.feedback = self.msg_cli.chn_name
			rsp.code = 200
			rsp.account = msg.account

			if self.mail_client.connect() != True:
				rsp.code = 404
				rsp.reason = "connect failed"
				self.mail_client.logout()
				self.mail_client = None

			if self.mail_client is not None and self.mail_client.login() != 0:
				rsp.code = 405
				rsp.reason = "login failed"
				self.mail_client.logout()
				self.mail_client = None

			msg_data = json.dumps({
				"msg_type": rsp.header.type,
				"msg_data": base64.b64encode(rsp.SerializeToString())
			})

			self.sch_msg_cli.send_msg(msg_data)

		elif msg.header.type == msg_pb2.TASK_ASSIGN_REQ:
			# 推送任务
			# TODO 检查参数
			# 从redis上获取邮件数据
			task_info = self.task_cli.get_task(msg.taskId)
			job_id = task_info.info["jobId"]
			mail_data = self.job_cli.get_maildata(job_id)
			if self.mail_client is None:
				ret = -5
				reason = 'no mail assigned to worker'
			else:
				(ret,reason) = self.mail_client.send_mail([task_info.info["to"]], mail_data)

			rsp = msg_pb2.TaskAssignRsp()
			rsp.header.type = msg_pb2.TASK_ASSIGN_RSP
			rsp.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			rsp.header.dest = msg.header.source
			rsp.header.feedback = self.msg_cli.chn_name
			rsp.taskId = msg.taskId
			rsp.code = ret
			rsp.reason = reason

			msg_data = json.dumps({
				"msg_type": rsp.header.type,
				"msg_data": base64.b64encode(rsp.SerializeToString())
			})

			# 发送反馈给scheduler
			self.sch_msg_cli.send_msg(msg_data)

			if ret == 0:
				logging.info('%s|%s|%d|success|%s|%s' %(self.node_info.info["nodeType"],
						self.node_info.info["nodeId"], msg.header.type, job_id, msg.taskId))
			else:
				logging.info('%s|%s|%d|failed|%s|%s' %(self.node_info.info["nodeType"],
						self.node_info.info["nodeId"], msg.header.type, job_id, msg.taskId))
		else:
			pass

		return True

	def run(self):

		logging.info("worker|%s|start" %(self.node_info.info["nodeId"]))

		global g_exist
		while g_exist and self.running:
			# 读取所有的消息
			msgs = self.msg_cli.get_msgs(-1)
			for msg in msgs:
				self.process_msg(msg)

			time.sleep(1)
		logging.info("worker|%s|exit" %(self.node_info.info["nodeId"]))

	def signal_stop(self):
		logging.info("worker|%s|got stop signal" %(self.node_info.info["nodeId"]))
		self.running = False

def sig_stop(sig_num, frame):
	global g_exist
	g_exist = False
	print "worker|%s|got stop signal" %("123")

if __name__ == "__main__":
	#signal.signal(signal.SIGINT, sig_stop)
	#signal.signal(signal.SIGTERM, sig_stop)
	#signal.signal(signal.SIGINT, lambda a,b : worker.signal_stop())
	#signal.signal(signal.SIGTERM, lambda a,b : worker.signal_stop())
	#signal.signal(signal.SIGKILL, lambda signum,frame: worker.signal_stop())
	if len(sys.argv) < 2:
		print "no enough arguments"
		sys.exit(0)

	worker_id = sys.argv[1]
	global_config["worker"]["nodeId"] = worker_id

	worker = Worker(global_config)
	worker.setDaemon(True)
	worker.start()

	worker.join()
