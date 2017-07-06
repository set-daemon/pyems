#!/usr/bin/env python
#-*- encoding=utf-8 -*-
'''
	purpose: 邮件发送的调度器
	author : set_daemonr@126.com
	date   : 2017-06-21
	history: 
'''

import sys
sys.path.insert(0, "../")
sys.path.insert(0, "./")
sys.path.insert(0, "../clients")
reload(sys)

import threading
import uuid
import base64
import time
import datetime
import logging
import random

from config import global_config
from msg_client import MsgClient
from job_client import JobClient,Job
from task_client import TaskClient
from mail_account_client import *
from node_client import NodeClient,NodeInfo
from proto import msg_pb2

'''
	Scheduler工作原理：
'''
class Scheduler(threading.Thread):
	def __del__(self):
		self.node_client.offline(self.node_info)	
	def __init__(self, config):
		super(Scheduler, self).__init__()

		self.config = config
		self.start_time = datetime.datetime.now()
		self.running = True

		self.node_info = NodeInfo()
		self.node_info.create(**{
			"nodeType": "scheduler",
			"nodeId": "1", # 固定为1,只可能有一个实例
			"instId": uuid.uuid1(),
			"msgScheme": self.config["scheduler"]["msg_channel"]["scheme"],
			"status": 1
		})
		self.node_info.set(**{
			"channel": "msgchn:%s:%s" %(self.node_info.info["nodeType"]
					  ,self.node_info.info["nodeId"]),
			"logTime": datetime.datetime.strftime(self.start_time, "%Y-%m-%d %H:%M:%S")
		})

		# 节点管理信息
		self.node_client = NodeClient(self.config["node_config"])
		self.node_client.sync_node(self.node_info)

		# 20170706 检查是否已经有scheduler节点启动
		q_node_info = self.node_client.get_node_info("scheduler:%s" %(self.config["worker"]["nodeId"]))
		if q_node_info is not None and len(q_node_info.info) > 0 and "nodeId" in q_node_info.info and q_node_info.info["status"] == "1":
			err_msg =  "已经有scheduler在%s启动" %(q_node_info.info["ip"])
			print err_msg
			raise Exception(err_msg)

		# 节点消息通道
		self.msg_cli = MsgClient(**{
			"scheme": self.config["scheduler"]["msg_channel"]["scheme"],
			"channel_name": self.node_info.info["channel"],
		})

		# Worker的消息通道
		self.worker_msg_cli = {}

		# 邮件帐号管理客户端
		self.mail_account_client = MailAccountClient(self.config["mails_config"])

		# Job管理客户端
		self.job_client = JobClient(self.config["job_config"])
		# Task管理客户端
		self.task_client = TaskClient(self.config["task_config"])

		logging.basicConfig(level=logging.INFO,
			format="%(asctime)s %(filename)s%(levelname)s %(message)s",
			datefmt='%a, %d %b %Y %H:%M:%S',
			filename="%s/%s_%s_%s.log" %(self.config["scheduler"]["logpath"],
					self.node_info.info["nodeType"], self.node_info.info["nodeId"], datetime.datetime.strftime(self.start_time, "%Y%m%d%H")),
			filemode='a')

	def get_worker_node(self, worker_id):
		worker_id = "worker:%s" %(worker_id.lstrip("worker:"))
		worker_node_info = self.node_client.get_node_info(worker_id)

		return worker_node_info

	def get_worker_msgcli(self, worker_id):
		worker_id = "worker:%s" %(worker_id.lstrip("worker:"))

		if worker_id not in self.worker_msg_cli:
			worker_node_info = self.get_worker_node(worker_id)
			if worker_node_info is None:
				return None

			sworker_msg_cli = MsgClient(**{
				"scheme": worker_node_info.info["msgScheme"],
				"channel_name": worker_node_info.info["channel"],
			})
			self.worker_msg_cli[worker_id] = sworker_msg_cli

		return self.worker_msg_cli[worker_id]

	def process_msg(self, msg):
		'''
		处理收到的msg
		'''
		if msg.header.type == msg_pb2.SCHEDULER_OFF_REQ:
			self.running = False
		if msg.header.type == msg_pb2.WORKER_OFF_REQ:
			req = msg_pb2.WorkerOffReq()
			req.header.type = msg_pb2.WORKER_OFF_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.dest = "worker:%s" %(msg.workerId)
			req.header.feedback = self.msg_cli.chn_name
			req.workerId = msg.workerId

			msg_data = json.dumps({
				"msg_type": req.header.type,
				"msg_data": base64.b64encode(req.SerializeToString())
			})

			worker_msg_cli = self.get_worker_msgcli(worker_id)
			if worker_msg_cli is not None:
				worker_msg_cli.send_msg(msg_data)

		elif msg.header.type == msg_pb2.MAIL_ON_REQ:
			# 同步到redis中
			account = MailAccount(**{
				"account": msg.account,
				"alias": msg.alias,
				"password": msg.password,
				"smtpHost": msg.smtpHost,
				"smtpPort": msg.smtpPort,
				"useSsl": msg.useSsl,
				"status": 1
			})
			self.mail_account_client.sync_account(**account.info)
			# 找到消息来源的会话通道
			source_node_info = self.node_client.get_node_info(msg.header.source)

			# 拼接相应信息
			rsp = msg_pb2.MailOnRsp()
			rsp.header.type = msg_pb2.MAIL_ON_RSP
			rsp.header.source = "scheduler"
			rsp.header.dest = msg.header.source
			rsp.header.feedback = self.msg_cli.chn_name
			rsp.account = msg.account
			rsp.code = 200
			rsp.reason = "success"

			msg_data = json.dumps({
				"msg_type": rsp.header.type,
				"msg_data": base64.b64encode(rsp.SerializeToString())
			})
			# 构建响应客户端
			rsp_client = MsgClient(**{
				"scheme" : source_node_info.info["msgScheme"],
				"channel_name": msg.header.feedback
			})
			rsp_client.send_msg(msg_data)

		elif msg.header.type == msg_pb2.MAIL_OFF_REQ:
			mail_account = MailAccount(**{
				"account": msg.account,
				"status": "-1"
			})
			self.mail_account_client.sync_account(**mail_account.info)

			# 找到消息来源的会话通道
			source_node_info = self.node_client.get_node_info(msg.header.source)
			
			# 拼接相应信息
			rsp = msg_pb2.MailOnRsp()
			rsp.header.type = msg_pb2.MAIL_OFF_RSP
			rsp.header.source = "scheduler"
			rsp.header.dest = msg.header.source
			rsp.header.feedback = self.msg_cli.chn_name
			rsp.account = msg.account
			rsp.code = 200
			rsp.reason = "success"

			msg_data = json.dumps({
				"msg_type": rsp.header.type,
				"msg_data": base64.b64encode(rsp.SerializeToString())
			})
			# 构建响应客户端
			rsp_client = MsgClient(**{
				"scheme" : source_node_info.info["msgScheme"],
				"channel_name": msg.header.feedback
			})
			rsp_client.send_msg(msg_data)
		elif msg.header.type == msg_pb2.MAIL_UPDATE_REQ:
			# 同步到redis中
			account = MailAccount(**{
				"account": msg.account,
				"useSsl": msg.useSsl,
			})
			if msg.alias != "":
				account.set(**{"alias":msg.alias})
			if msg.password != "":
				account.set(**{"password":msg.password})
			if msg.smtpHost != "":
				account.set(**{"smtpHost":msg.smtpHost})
			if msg.smtpPort != "":
				account.set(**{"smtpPort":msg.smtpPort})
		
			self.mail_account_client.sync_account(**account.info)
			# 找到消息来源的会话通道
			source_node_info = self.node_client.get_node_info(msg.header.source)

			# 拼接相应信息
			rsp = msg_pb2.MailOnRsp()
			rsp.header.type = msg_pb2.MAIL_UPDATE_RSP
			rsp.header.source = "scheduler"
			rsp.header.dest = msg.header.source
			rsp.header.feedback = self.msg_cli.chn_name
			rsp.account = msg.account
			rsp.code = 200
			rsp.reason = "success"

			msg_data = json.dumps({
				"msg_type": rsp.header.type,
				"msg_data": base64.b64encode(rsp.SerializeToString())
			})
			# 构建响应客户端
			rsp_client = MsgClient(**{
				"scheme" : source_node_info.info["msgScheme"],
				"channel_name": msg.header.feedback
			})
			rsp_client.send_msg(msg_data)

		elif msg.header.type == msg_pb2.MAIL_STATUS_REQ:
			account = msg.account	
			account_info = self.mail_account_client.get_account(account)
			# 找到消息来源的会话通道
			source_node_info = self.node_client.get_node_info(msg.header.source)

			# 拼接相应信息
			rsp = msg_pb2.MailStatusRsp()
			rsp.header.type = msg_pb2.MAIL_STATUS_RSP
			rsp.header.source = "scheduler"
			rsp.header.dest = msg.header.source
			rsp.header.feedback = self.msg_cli.chn_name

			rsp.account = msg.account
			rsp.code = 200
			rsp.reason = "success"

			rsp.alias = account_info.info["alias"]
			rsp.password = account_info.info["password"]
			rsp.smtpHost = account_info.info["smtpHost"]
			rsp.smtpPort = account_info.info["smtpPort"]
			rsp.useSsl = True
			if account_info.info["useSsl"] == "False":
				rsp.useSsl = False

			rsp.status = int(account_info.info["status"])

			msg_data = json.dumps({
				"msg_type": rsp.header.type,
				"msg_data": base64.b64encode(rsp.SerializeToString())
			})
			# 构建响应客户端
			rsp_client = MsgClient(**{
				"scheme" : source_node_info.info["msgScheme"],
				"channel_name": msg.header.feedback
			})
			rsp_client.send_msg(msg_data)

		elif msg.header.type == msg_pb2.JOB_ON_REQ:
			job_id = str(uuid.uuid1())
			job = Job()
			job.set(**{
				"owner": msg.owner,
				"jobId": job_id,
				"senderAccounts": ",".join(msg.senderAccounts),
				"toAccounts": ",".join(msg.toAccounts),
				"mailData": msg.mailData,
				"status": 1,
				"onTime": datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H:%M:%S")
			})
			self.job_client.sync_job(job)
			# 将待发送列表写入
			self.job_client.update_sending_list(job_id, msg.toAccounts)
			
			# 找到消息来源的会话通道
			source_node_info = self.node_client.get_node_info(msg.header.source)

			# 拼接相应信息
			rsp = msg_pb2.JobOnRsp()
			rsp.header.type = msg_pb2.JOB_ON_RSP
			rsp.header.source = "scheduler"
			rsp.header.dest = msg.header.source
			rsp.header.feedback = self.msg_cli.chn_name

			rsp.jobId = job_id
			rsp.code = 200
			rsp.reason = "success"

			msg_data = json.dumps({
				"msg_type": rsp.header.type,
				"msg_data": base64.b64encode(rsp.SerializeToString())
			})
			# 构建响应客户端
			rsp_client = MsgClient(**{
				"scheme" : source_node_info.info["msgScheme"],
				"channel_name": msg.header.feedback
			})
			rsp_client.send_msg(msg_data)

		elif msg.header.type == msg_pb2.JOB_OFF_REQ:
			job = Job()
			job.set({
				"jobId": msg.jobId,
				"status": 0
			})
			self.job_client.sync_job(job)

			# 找到消息来源的会话通道
			source_node_info = self.node_client.get_node_info(msg.header.source)

			# 拼接相应信息
			rsp = msg_pb2.JobOffRsp()
			rsp.header.type = msg_pb2.JOB_OFF_RSP
			rsp.header.source = "scheduler"
			rsp.header.dest = msg.header.source
			rsp.header.feedback = self.msg_cli.chn_name

			rsp.jobId = job_id
			rsp.code = 200
			rsp.reason = "success"

			msg_data = json.dumps({
				"msg_type": rsp.header.type,
				"msg_data": base64.b64encode(rsp.SerializeToString())
			})
			# 构建响应客户端
			rsp_client = MsgClient(**{
				"scheme" : source_node_info.info["msgScheme"],
				"channel_name": msg.header.feedback
			})
			rsp_client.send_msg(msg_data)
		elif msg.header.type == msg_pb2.JOB_UPDATE_REQ:
			job = Job()
			job.set(**{
				"owner": msg.owner,
				"jobId": msg.jobId,
				"toAccounts": ",".join(msg.toAccounts),
				"mailData": msg.mailData,
			})
			#print msg.senderAccounts
			if len(msg.senderAccounts) > 0:
				job.set(**{ "senderAccounts": ",".join(msg.senderAccounts)})	
			#print msg.toAccounts
			if len(msg.toAccounts) > 0:
				job.set(**{ "toAccounts": ",".join(msg.toAccounts)})	
			if msg.mailData != "":
				job.set(**{ "mailData": msg.mailData})

			self.job_client.sync_job(job)
			# 将待发送列表写入
			self.job_client.update_sending_list(msg.jobId, msg.toAccounts)
			
			# 找到消息来源的会话通道
			source_node_info = self.node_client.get_node_info(msg.header.source)

			# 拼接相应信息
			rsp = msg_pb2.JobUpdateRsp()
			rsp.header.type = msg_pb2.JOB_UPDATE_RSP
			rsp.header.source = "scheduler"
			rsp.header.dest = msg.header.source
			rsp.header.feedback = self.msg_cli.chn_name

			rsp.jobId = msg.jobId
			rsp.code = 200
			rsp.reason = "success"

			msg_data = json.dumps({
				"msg_type": rsp.header.type,
				"msg_data": base64.b64encode(rsp.SerializeToString())
			})
			# 构建响应客户端
			rsp_client = MsgClient(**{
				"scheme" : source_node_info.info["msgScheme"],
				"channel_name": msg.header.feedback
			})
			rsp_client.send_msg(msg_data)

		
		elif msg.header.type == msg_pb2.JOB_STATUS_REQ:
			status = self.job_client.query_job_status(msg.jobId)		
			# 找到消息来源的会话通道
			source_node_info = self.node_client.get_node_info(msg.header.source)

			# 拼接相应信息
			rsp = msg_pb2.JobStatusRsp()
			rsp.header.type = msg_pb2.JOB_STATUS_RSP
			rsp.header.source = "scheduler"
			rsp.header.dest = msg.header.source
			rsp.header.feedback = self.msg_cli.chn_name

			rsp.jobId = msg.jobId
			rsp.code = 200
			rsp.reason = "success"

			rsp.completedNum = status["sent"]
			rsp.successNum = 0
			rsp.totalNum = status["total"]

			msg_data = json.dumps({
				"msg_type": rsp.header.type,
				"msg_data": base64.b64encode(rsp.SerializeToString())
			})
			# 构建响应客户端
			rsp_client = MsgClient(**{
				"scheme" : source_node_info.info["msgScheme"],
				"channel_name": msg.header.feedback
			})
			rsp_client.send_msg(msg_data)
		elif msg.header.type == msg_pb2.JOB_LIST_REQ:
			job_list = self.job_client.get_joblist()
			# 找到消息来源的会话通道
			source_node_info = self.node_client.get_node_info(msg.header.source)

			# 拼接相应信息
			rsp = msg_pb2.JobListRsp()
			rsp.header.type = msg_pb2.JOB_LIST_RSP
			rsp.header.source = "scheduler"
			rsp.header.dest = msg.header.source
			rsp.header.feedback = self.msg_cli.chn_name

			rsp.code = 200
			rsp.reason = "success"
			rsp.jobList.extend(job_list)

			msg_data = json.dumps({
				"msg_type": rsp.header.type,
				"msg_data": base64.b64encode(rsp.SerializeToString())
			})
			# 构建响应客户端
			rsp_client = MsgClient(**{
				"scheme" : source_node_info.info["msgScheme"],
				"channel_name": msg.header.feedback
			})
			rsp_client.send_msg(msg_data)
		
		elif msg.header.type == msg_pb2.WORKER_LIST_REQ:
			pass
		elif msg.header.type == msg_pb2.WORKER_STATUS_REQ:
			pass
		elif msg.header.type == msg_pb2.MAIL_ALTER_RSP:
			if msg.code != 200:
				now = datetime.datetime.now()
				self.mail_account_client.sync_account_worker(**{
					"account": msg.account,
					"workerId": msg.header.source,
					"status": "0",
					"lastFailTime": datetime.datetime.strftime(now, "%Y-%m-%d %H:%M:%S")
				})
				self.mail_account_client.sync_account(**{
					"account": msg.account,
					"status": "0",
					"lastFailTime": datetime.datetime.strftime(now, "%Y-%m-%d %H:%M:%S")
				})
				logging.info("|%s|%s|%s|%s|%s" %(
					"mail_alter", msg.account, "failed", msg.header.source,
					msg.reason
				))
			else:
				logging.info("|%s|%s|%s|%s|%s" %(
					"mail_alter", msg.account, "success", msg.header.source,
					msg.reason
				))

		elif msg.header.type == msg_pb2.TASK_ASSIGN_RSP:
			# 更新任务状态
			task_info = self.task_client.get_task(msg.taskId)
			job_id = task_info.info["jobId"]
			to_account = task_info.info["to"]

			# 20170630 更新worker状态
			worker_node = NodeInfo(**{
				"nodeType": "worker",
				"nodeId": "%s" %(msg.header.source.lstrip('worker:'))
			})
			worker_node.set(**{
				"free": "1",
				"taskId": ""
			})
			self.node_client.sync_node(worker_node)

			if msg.code == 0:
				self.task_client.sync_task(**{
					"taskId": msg.taskId,
					"status": 0
				})
				self.job_client.sending_done_once(job_id, to_account)

				logging.info("|%s|%s|%s|%s|%s|%s|%s|%s" %(
					"mail_sent", "success", job_id, msg.taskId,
					task_info.info["from"], task_info.info["to"], msg.reason,
					msg.header.source
				))
			elif msg.code == -2:
				# 接收者邮箱拒绝，算发送失败，可能不需要再发送
				self.task_client.sync_task(**{
					"taskId": msg.taskId,
					"status": 0
				})
				self.job_client.sending_done_once(job_id, to_account)
				
				logging.info("|%s|%s|%s|%s|%s|%s|%s|no_send_again|%s" %(
					"mail_sent", "failed", job_id, msg.taskId,
					task_info.info["from"], task_info.info["to"], msg.reason,
					msg.header.source
				))

			else:
				# 发件箱出问题，还需要继续发送
				self.task_client.sync_task(**{
					"taskId": msg.taskId,
					"status": 3
				})

				task = self.task_client.get_task(msg.taskId)

				now = datetime.datetime.now()
				self.mail_account_client.sync_account_worker(**{
					"account": task.info["from"],
					"workerId": msg.header.source,
					"status": "0",
					"lastFailTime": datetime.datetime.strftime(now, "%Y-%m-%d %H:%M:%S")
				})
				self.mail_account_client.sync_account(**{
					"account": task.info["from"],
					"status": "0",
					"lastFailTime": datetime.datetime.strftime(now, "%Y-%m-%d %H:%M:%S"),
					"failCode": msg.code
				})

				logging.info("|%s|%s|%s|%s|%s|%s|%s|send_again_later|%s|%d" %(
					"mail_sent", "failed", job_id, msg.taskId,
					task_info.info["from"], task_info.info["to"], msg.reason,
					msg.header.source, msg.code
				))
		else:
			pass

	def dispatch_job_senders(self, job_id, job_info=None):
		'''
		将job的发送account分布到各个可用worker上
		'''
		if job_info is None:
			job_info = self.job_client.get_job_running_data(job_id)

		# 查找所有的worker及生成消息通道
		worker_list = self.node_client.get_nodes("worker")
		for worker_id in worker_list:
			if worker_id in self.worker_msg_cli:
				continue

			worker_node_info = self.node_client.get_node_info(worker_id)
			self.worker_msg_cli[worker_id] = MsgClient(**{
				"scheme": worker_node_info.info["msgScheme"],
				"channel_name": worker_node_info.info["channel"],
			})

		senderAccounts = job_info["senderAccounts"].split(',')
		# 查询所有未工作、且状态正常的senders信息
		unworking_account_info = self.mail_account_client.query_unworking_account_selectable_workers(senderAccounts, worker_list)

		# 计算最后可以工作的workers和account
		available_workers = set()
		available_accounts = []
		for account in unworking_account_info.keys():
			can_work_on = unworking_account_info[account]
			if len(can_work_on) <= 0:
				continue
			available_accounts.append(account)
			available_workers = available_workers | set(can_work_on)

		worker_list = list(available_workers)

		worker_seq = 0
		for sender in available_accounts:
			# 求剩下的worker可用列表
			account_can_work_on = unworking_account_info[sender]
			remain_can_work_on = list(set(account_can_work_on) & set(worker_list))
			if len(remain_can_work_on) <= 0:
				break

			rand_seq = random.randint(0, len(remain_can_work_on)-1)
			worker_id = remain_can_work_on[rand_seq]
			worker_list.remove(worker_id)

			req = msg_pb2.MailAlterReq()
			req.header.type = msg_pb2.MAIL_ALTER_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.feedback = self.node_info.info["channel"]
			worker_cli = self.worker_msg_cli[worker_id]

			req.header.dest = worker_id

			req.account = sender

			msg_data = json.dumps({
				"msg_type": req.header.type,
				"msg_data": base64.b64encode(req.SerializeToString())
			})
			worker_cli.send_msg(msg_data)

			# 更新邮箱当前工作的worker，注意：worker可能不能正常登录该邮箱导致失败，而且是异步返回结果，这里暂时先更新
			now = datetime.datetime.now()
			self.mail_account_client.sync_account_worker(**{
				"account": sender,
				"workerId": worker_id,
				"status": 1,
				"onTime": datetime.datetime.strftime(now, "%Y-%m-%d %H:%M:%S"),
			})

			# 更新邮箱状态
			self.mail_account_client.sync_account(**{
				"account": sender,
				"status": 1
			})

	def stupid_schedule(self):
		'''
		傻瓜式调度：每次只执行一个job（在没有完整思路前，先用一种方式跑起来）
		'''
		running_job = self.job_client.get_running_job()
		if running_job is None or len(running_job) <= 0 or running_job["status"] == 0:	
			jobs = self.job_client.query_suitable_jobs()
			# 选择的参考因素：总发送量、上线时长、完成度
			#   优先
			if len(jobs) <= 0:
				return True

			ok_job = None
			for job_id in jobs.keys():
				job = jobs[job_id]

				if ok_job is None:
					ok_job = job
					continue
				ok_t = datetime.datetime.strptime(ok_job["onTime"], "%Y-%m-%d %H:%M:%S")	
				job_t = datetime.datetime.strptime(job["onTime"], "%Y-%m-%d %H:%M:%S")	
				# job的开始时间，超过2天的优先
				if (job_t - ok_t).days <= -2:
					ok_job = job
					continue
				# 剩余率小的优先：尽快结束该job，比如(5/10)和(95/100)，应优先开启后面的
				ok_sending_rate = ok_job["sending"] / ok_job["total"]
				job_sending_rate = job["sending"] / job["total"]
				if ok_sending_rate > job_sending_rate:
					ok_job = job
					continue
				# 总量小的优先：例如(500/1000)和(50/100)，剩余率都一样，则取总量小的发送
				if job["total"] < ok_job["total"]:
					ok_job = job

			if ok_job is not None:
				# 重新上线该job
				now = datetime.datetime.now()
				self.job_client.sync_running_job(**{
					"status": 1,
					"jobId": ok_job["jobId"],
					"startTime": "%s" %(datetime.datetime.strftime(now, "%Y-%m-%d %H:%M:%S"))
				})
				# 将发送箱全部上线工作
				self.dispatch_job_senders(ok_job["jobId"], ok_job)
		else:
			# 如果一个job的执行时间超过半个小时，则需要轮换	
			now = datetime.datetime.now()
			start_time = datetime.datetime.strptime(running_job["startTime"], "%Y-%m-%d %H:%M:%S")	
			if (now - start_time).total_seconds() >= 60*30:
				self.job_client.sync_running_job(**{"status": 0})	
				# 下线所有的worker
			else:
				pass

		# @@@ 布置任务
		# 重新查询一遍执行中的job
		running_job = self.job_client.get_running_job()
		if running_job is None or len(running_job) <= 0 or running_job["status"] == 0:	
			return True

		# 获取job信息
		job_info = self.job_client.get_job_running_data(running_job["jobId"])
		if job_info["sent"] == job_info["total"]:
			return True

		# 部署邮箱到各worker上
		self.dispatch_job_senders(running_job["jobId"], job_info)

		# 获取当前job可用的邮箱
		job_senders = job_info["senderAccounts"].split(',')
		available_accounts = self.mail_account_client.query_all_available_accounts(*job_senders)
		working_accounts = self.mail_account_client.query_all_working_accounts(*available_accounts)
		# 20170630 判断worker上是否有任务未完成，只有空闲的worker才能工作
		for working_account in working_accounts.keys():
			work_id = working_accounts[working_account]
			work_node_info = self.get_worker_node(work_id)
			if work_node_info is None or work_node_info.info["taskId"] != "" or work_node_info.info["free"] != "1":
				del working_accounts[working_account]	
				continue

		working_account_list = working_accounts.keys()
		#print working_account_list
		# 查询job未完成的任务
		task_ids = self.task_client.query_job_tasks(running_job["jobId"])
		tasks = {}
		for task_id in task_ids:
			task = self.task_client.get_task(task_id)
			if task.info["status"] == "0":
				# 已经结束
				#print "task %s already ok" %(task_id)
				continue
			elif task.info["status"] == "1":
				# 正在发送中，需要等待
				#print "task %s is on sending" %(task_id)
				# 20170630 如果处理时间过长，超过半个小时，则认为任务失败
				need_resend = False
				pt_now = datetime.datetime.now()
				if "onTime" in task.info: # 为了兼容以前未设置该值的任务
					on_time = datetime.datetime.strptime(task.info["onTime"], "%Y-%m-%d %H:%M:%S")
					if (pt_now - on_time).total_seconds() > 30*60:
						need_resend = True
				else:
					# 暂时更新该字段，否则当前刚发布的任务可能会被重新发送
					self.task_client.sync_task(**{
						"taskId": task_id,
						"onTime": datetime.datetime.strftime(pt_now, "%Y-%m-%d %H:%M:%S")
					})

				if need_resend is True:
					self.task_client.sync_task(**{
						"taskId": task_id,
						"status": "3"
					})

				continue
			elif task.info["status"] in ["2", "3"]:
				# 任务失败，则需要重新分配到下一个mail
				previous_sender = task.info["from"]
				new_sender = None
				for i in range(0, len(working_account_list)):
					if working_account_list[i] != previous_sender:
						new_sender = working_account_list[i]
						break;
				if new_sender is not None:
					working_account_list.remove(new_sender)
					# 更新任务
					self.task_client.sync_task(**{
						"taskId": task_id,
						"from": new_sender,
						"status": "1"
					})
					# 更新账号发送时间
					send_time = datetime.datetime.now()
					self.mail_account_client.sync_account(**{
						"account": new_sender,
						"lastSendTime": datetime.datetime.strftime(send_time, "%Y-%m-%d %H:%M:%S")
					})

					# 发布任务到worker
					worker_id = working_accounts[new_sender]

					# 20170630 更新worker信息
					worker_node_info = NodeInfo(**{
						"nodeType": "worker",
						"nodeId": "%s" %(worker_id.lstrip('worker:'))
					})
					worker_node_info.set(**{
						"taskId": task_id,
						"free": "0"
					})
					self.node_client.sync_node(worker_node_info)

					worker_msg_cli = self.get_worker_msgcli(worker_id)

					req = msg_pb2.TaskAssignReq()
					req.header.type = msg_pb2.TASK_ASSIGN_REQ
					req.header.source = "%s:%s" %(self.node_info.info["nodeType"],
									self.node_info.info["nodeId"])
					req.header.dest = "worker:%s" %(worker_id.lstrip('worker:'))
					req.header.feedback = self.node_info.info["channel"]
					req.taskId = task_id
					req.jobId = running_job["jobId"]
					msg_data = json.dumps({
						"msg_type": req.header.type,
						"msg_data": base64.b64encode(req.SerializeToString())	
					})
					worker_msg_cli.send_msg(msg_data)

		#print working_account_list
		# 生成新的任务
		for account in working_account_list:
			worker_id = working_accounts[account]
			worker_msg_cli = self.get_worker_msgcli(worker_id)

			# 取出新的待收件邮箱
			to_account = self.job_client.fetch_next_one(running_job["jobId"])
			if to_account is None or to_account == "":
				#print "there is no account to sent"
				break

			# 生成任务
			task_id = str(uuid.uuid1())
			gen_time = datetime.datetime.now()
			self.task_client.sync_task(**{
				"taskId": task_id,
				"jobId": running_job["jobId"],
				"from": account,
				"to": to_account,
				"status": "1",
				"onTime": datetime.datetime.strftime(gen_time, "%Y-%m-%d %H:%M:%S")
			})
			# 更新job的task集
			self.task_client.sync_job_task(running_job["jobId"], task_id)

			# 20170630 更新worker信息
			worker_node_info = NodeInfo(**{
				"nodeType": "worker",
				"nodeId": "%s" %(worker_id.lstrip('worker:'))
			})
			worker_node_info.set(**{
				"taskId": task_id,
				"free": "0"
			})
			self.node_client.sync_node(worker_node_info)

			# 更新账号发送时间
			send_time = datetime.datetime.now()
			self.mail_account_client.sync_account(**{
				"account": account,
				"lastSendTime": datetime.datetime.strftime(send_time, "%Y-%m-%d %H:%M:%S")
			})

			req = msg_pb2.TaskAssignReq()
			req.header.type = msg_pb2.TASK_ASSIGN_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.dest = "worker:%s" %(worker_id)
			req.header.feedback = self.node_info.info["channel"]
			req.taskId = task_id
			req.jobId = running_job["jobId"]
			msg_data = json.dumps({
				"msg_type": req.header.type,
				"msg_data": base64.b64encode(req.SerializeToString())	
			})
			worker_msg_cli.send_msg(msg_data)

	def run(self):
		logging.info("%s|%s|start_running"
				%(self.node_info.info["nodeType"],self.node_info.info["nodeId"]))
		while self.running:
			# 读取所有的消息
			msgs = self.msg_cli.get_msgs(-1)
			for msg in msgs:
				self.process_msg(msg)

			self.stupid_schedule()

			time.sleep(1)

		logging.info("%s|%s|stop_running"
				%(self.node_info.info["nodeType"],self.node_info.info["nodeId"]))

if __name__ == "__main__":
	scheduler = Scheduler(global_config)
	scheduler.setDaemon(True)

	scheduler.start()

	scheduler.join()
