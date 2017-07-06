#!/usr/bin/env python
#-*- encoding=utf-8 -*-

'''
    purpose: 实现命令行下的EMS（邮件营销系统）的管理
    author : set_daemon@126.com
    date   : 2017-06-23 
    history: 
'''

import sys
file_dir = sys.path[0]
sys.path.insert(0, file_dir + "/./")
sys.path.insert(0, file_dir + "/../")
sys.path.insert(0, file_dir + "/../common")
sys.path.insert(0, file_dir + "/../clients")
reload(sys)

import threading
import uuid
import json
import base64
import datetime

from msg_client import MsgClient
from proto import msg_pb2
from utils import *
from mail_creator import MailCreator
from node_client import NodeClient,NodeInfo

class MailShell(object):
	'''
	通过scheduler来最终执行命令，包括：
	1）邮箱管理：上线、更新、下线，查看邮箱状态
	2）Job管理：上线、更新、下线，查看Job状态
	3）任务管理：（由scheduler自动调配），可通过Job查询任务执行状态
	4）Worker管理：上线、下线、更新工作邮箱，查看worker信息
	'''
	cmds = [
		"quit", "exit", "help", "mail", "job", "worker", "msg", "scheduler"
	]

	def __del__(self):
		self.node_client.destroy(self.node_info)

	def __init__(self, config):
		self.config = config
		self.start_time = datetime.datetime.now()

		self.node_info = NodeInfo()
		self.node_info.create(**{
			"nodeType": "shell",
			"nodeId": uuid.uuid1(),
			"msgScheme": self.config["shell_config"]["scheme"],
			"status": 1
		})
		self.node_info.set(**{
			"channel": "msgchn:%s:%s" %(self.node_info.info["nodeType"]
					  ,self.node_info.info["nodeId"]),
			"instId": self.node_info.info["nodeId"],
			"logTime": datetime.datetime.strftime(self.start_time, "%Y-%m-%d %H:%M:%S")
		})

		# 节点管理信息
		self.node_client = NodeClient(self.config["node_config"])
		self.node_client.sync_node(self.node_info)

		# 节点消息通道
		self.msg_cli = MsgClient(**{
			"scheme": self.config["shell_config"]["scheme"],
			"channel_name": self.node_info.info["channel"],
		})

		# scheduler消息通道
		node_keys = self.node_client.get_nodes("scheduler")
		node_info = None
		for key in node_keys:
			key = key.lstrip('node:')
			node_info = self.node_client.get_node_info(key)
			if node_info.info["status"] == "1":
				break
		self.sch_msg_cli = MsgClient(**{
		"scheme": node_info.info["msgScheme"],
		"channel_name": node_info.info["channel"]
	})

	@staticmethod
	def arg_parse(args, flags_predef):
		'''
		解析有名和无名参数
		参数格式 -n huang -a 12
		flags格式，参数名之间以分号分隔，举例：
			n/name:- 表示参数名称为name，简写为n，参数值取到下一个以"-"开始的参数为止
			n/name:+ 表示参数名为name，简写为n，参数为1个，以+的个数为参数个数
			n/name: 表示参数名为name，简写为n，无参数
		'''
		arg_defs = {}
		flags = flags_predef.split(';')
		for flag in flags:
			segs = flag.split(':')
			arg_name_segs = segs[0].split('/')
			arg_num = 0
			end_flag = ''
			if segs[1] == '-':
				end_flag = '-'
				arg_num = -1
			elif segs[1] == '':
				arg_num = 0
			else:
				arg_num = len(segs[1].split('+'))
				# split的段落个数要比+个数多1
				if arg_num > 0:
					arg_num -= 1
			for arg_name in arg_name_segs:
				arg_defs[arg_name] = {
					'end_flag': end_flag,
					'arg_num': arg_num
				}

		kv = {
			"__NN":[]
		}
		i = 0
		arg_len = len(args)
		while i < arg_len:
			arg = args[i]
			if arg.startswith('-'):
				arg_name = arg.lstrip('-')
				if arg_name in arg_defs:
					arg_def = arg_defs[arg_name]
					kv[arg_name] = []
					# 判断结束符
					if arg_def['end_flag'] == '-':
						# 该参数名的值取到下一个-开始
						i += 1
						while i < arg_len and args[i].startswith('-') != True:
							arg_v = args[i]	
							kv[arg_name].append(arg_v)
							i += i
					else:
						# 按数量取
						k = 0
						i += 1
						while i < arg_len and k < arg_def["arg_num"]:
							arg_v = args[i]
							kv[arg_name].append(arg_v)
							k += 1
							i += 1
				else: # 不在定义范围，则将参数值取完直到参数为-开头
					kv[arg_name] = []
					i += 1
					while i < arg_len and args[i].startswith('-') != True:
						arg_v = args[i]
						kv[arg_name].append(arg_v)
						i += 1	
			else:
				kv["__NN"].append(arg)
				i += 1
		return kv

	def parse_cmd(self, cmdline):
		segs = cmdline.strip('\t \n').split(' ')
		seg_len = len(segs)
		if seg_len < 1:
			return None
		cmd_name = segs[0]	
		if cmd_name not in MailShell.cmds:
			return None

		cmd = {
			"name": cmd_name,
			"op": "",
			"params": None
		}

		if cmd_name == "quit" or cmd_name == "exit" or cmd_name == "help":
			pass
		else:
			if seg_len < 2:
				return None
			cmd["op"] = segs[1]
			if cmd_name == "mail":
				cmd["params"] = MailShell.arg_parse(segs[2:], 'a/account:+;A/alias:+;p/password:+;h/host:+;P/port:+;s/ssl:+')
			elif cmd_name == "job":
				cmd["params"] = MailShell.arg_parse(segs[2:], 'j/job:+;f/job_file:+')
			elif cmd_name == "worker":
				cmd["params"] = MailShell.arg_parse(segs[2:], 'w/worker:+')
			elif cmd_name == "scheduler":
				pass
			elif cmd_name == "msg":
				#cmd["params"] = MailShell.arg_parse(segs[2:], '')
				pass
			else:
				pass

		return cmd

	def exec_mail_cmd(self, cmd):
		op = cmd["op"]

		msg_data = ""
		if op == "on":
			# 检查及合并参数
			req = msg_pb2.MailOnReq()
			req.header.type = msg_pb2.MAIL_ON_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.dest = "scheduler"
			req.header.feedback = self.msg_cli.chn_name

			for key in cmd["params"].keys():
				param_v = cmd["params"][key]
				if key in ["a", "account"]:
					req.account = param_v[0]
				elif key in ["A", "alias"]:
					req.alias = unicode(param_v[0],"utf-8")
				elif key in ["p", "password"]:
					req.password = param_v[0]
				elif key in ["h", "host"]:
					req.smtpHost = param_v[0]
				elif key in ["P", "port"]:
					req.smtpPort = param_v[0]
				elif key in ["s", "ssl"]:
					ssl = int(param_v[0])
					req.useSsl = (ssl == 1)

			msg_data = json.dumps({
				"msg_type": msg_pb2.MAIL_ON_REQ,
				"msg_data": base64.b64encode(req.SerializeToString())
			})

		elif op == "off":
			req = msg_pb2.MailOffReq()
			req.header.type = msg_pb2.MAIL_OFF_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.dest = "scheduler"
			req.header.feedback = self.msg_cli.chn_name

			for key in cmd["params"].keys():
				param_v = cmd["params"][key]
				if key in ["a", "account"]:
					req.account = param_v[0]

			msg_data = json.dumps({
				"msg_type": msg_pb2.MAIL_OFF_REQ,
				"msg_data": base64.b64encode(req.SerializeToString())
			})

		elif op == "update":
			req = msg_pb2.MailUpdateReq()
			req.header.type = msg_pb2.MAIL_UPDATE_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.dest = "scheduler"
			req.header.feedback = self.msg_cli.chn_name

			for key in cmd["params"].keys():
				param_v = cmd["params"][key]
				if key in ["a", "account"]:
					req.account = param_v[0]
				elif key in ["A", "alias"]:
					req.alias = unicode(param_v[0], 'utf-8')
				elif key in ["p", "password"]:
					req.password = param_v[0]
				elif key in ["h", "host"]:
					req.smtpHost = param_v[0]
				elif key in ["P", "port"]:
					req.smtpPort = param_v[0]
				elif key in ["s", "ssl"]:
					if param_v[0] == "0":
						req.useSsl = False
					else:
						req.useSsl = True

			msg_data = json.dumps({
				"msg_type": msg_pb2.MAIL_UPDATE_REQ,
				"msg_data": base64.b64encode(req.SerializeToString())
			})

		elif op == "status":
			req = msg_pb2.MailStatusReq()
			req.header.type = msg_pb2.MAIL_STATUS_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.dest = "scheduler"
			req.header.feedback = self.msg_cli.chn_name
			for key in cmd["params"].keys():
				param_v = cmd["params"][key]
				if key in ["a", "account"]:
					req.account = param_v[0]

			msg_data = json.dumps({
				"msg_type": msg_pb2.MAIL_STATUS_REQ,
				"msg_data": base64.b64encode(req.SerializeToString())
			})

		else:
			pass

		self.sch_msg_cli.send_msg(msg_data)
	def exec_job_cmd(self, cmd):
		op = cmd["op"]

		msg_data = ""
		if op == "on":
			req = msg_pb2.JobOnReq()
			req.header.type = msg_pb2.JOB_ON_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.dest = "scheduler"
			req.header.feedback = self.msg_cli.chn_name
			for key in cmd["params"].keys():
				param_v = cmd["params"][key]
				if key in ["f", "job_file"]:
					# 读取job文件
					job_file = param_v[0]
					file_data = open(job_file, 'r').read()
					jobj = json.loads(file_data)
					req.owner = jobj["owner"]
					senders = get_mails(jobj["senders"])
					req.senderAccounts.extend(senders)
					to_accounts = get_mails(jobj["to"])
					req.toAccounts.extend(to_accounts)
					mail_desc_file = jobj["mail_desc_file"]
					mail_creator = MailCreator()
					mail_creator.load(mail_desc_file)
					req.mailData = mail_creator.to_msg()

			msg_data = json.dumps({
				"msg_type": msg_pb2.JOB_ON_REQ,
				"msg_data": base64.b64encode(req.SerializeToString())
			})

		elif op == "off":
			req = msg_pb2.JobOffReq()
			req.header.type = msg_pb2.JOB_OFF_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.dest = "scheduler"
			req.header.feedback = self.msg_cli.chn_name
			for key in cmd["params"].keys():
				param_v = cmd["params"][key]
				if key in ["j", "job_id"]:
					job_id = param_v[0]
					req.jobId = job_id

			msg_data = json.dumps({
				"msg_type": msg_pb2.JOB_OFF_REQ,
				"msg_data": base64.b64encode(req.SerializeToString())
			})

		elif op == "update":
			req = msg_pb2.JobUpdateReq()
			req.header.type = msg_pb2.JOB_UPDATE_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.dest = "scheduler"
			req.header.feedback = self.msg_cli.chn_name
			for key in cmd["params"].keys():
				param_v = cmd["params"][key]
				if key in ["f", "job_file"]:
					# 读取job文件
					job_file = param_v[0]
					file_data = open(job_file, 'r').read()
					jobj = json.loads(file_data)
					req.owner = jobj["owner"]
					senders = get_mails(jobj["senders"])
					req.senderAccounts.extend(senders)
					to_accounts = get_mails(jobj["to"])
					req.toAccounts.extend(to_accounts)
					mail_desc_file = jobj["mail_desc_file"]
					mail_creator = MailCreator()
					mail_creator.load(mail_desc_file)
					req.mailData = mail_creator.to_msg()
				elif key in ["j", "job_id"]:
					req.jobId = param_v[0]

			msg_data = json.dumps({
				"msg_type": msg_pb2.JOB_UPDATE_REQ,
				"msg_data": base64.b64encode(req.SerializeToString())
			})
		elif op == "status":
			req = msg_pb2.JobStatusReq()
			req.header.type = msg_pb2.JOB_STATUS_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.dest = "scheduler"
			req.header.feedback = self.msg_cli.chn_name
			for key in cmd["params"].keys():
				param_v = cmd["params"][key]
				if key in ["j", "job_id"]:
					job_id = param_v[0]
					req.jobId = job_id
			# 检查参数

			msg_data = json.dumps({
				"msg_type": msg_pb2.JOB_STATUS_REQ,
				"msg_data": base64.b64encode(req.SerializeToString())
			})
		elif op == "list":
			req = msg_pb2.JobListReq()
			req.header.type = msg_pb2.JOB_LIST_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.dest = "scheduler"
			req.header.feedback = self.msg_cli.chn_name

			msg_data = json.dumps({
				"msg_type": req.header.type,
				"msg_data": base64.b64encode(req.SerializeToString())
			})
		else:
			pass
		
		self.sch_msg_cli.send_msg(msg_data)

	def exec_worker_cmd(self, cmd):
		msg_data = ""
		op = cmd["op"]

		if op == "off":
			req = msg_pb2.WorkerOffReq()
			req.header.type = msg_pb2.WORKER_OFF_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.dest = "scheduler"
			req.header.feedback = self.msg_cli.chn_name

			for key in cmd["params"].keys():
				param_v = cmd["params"][key]
				if key in ["w", "worker_id"]:
					worker_id = param_v[0]
					req.workerId = worker_id

			msg_data = json.dumps({
				"msg_type": req.header.type,
				"msg_data": base64.b64encode(req.SerializeToString())
			})
	
		else:
			pass

		if msg_data != "":
			self.sch_msg_cli.send_msg(msg_data)

	def exec_scheduler_cmd(self, cmd):
		msg_data = ""
		op = cmd["op"]

		if op == "off":
			req = msg_pb2.SchedulerOffReq()
			req.header.type = msg_pb2.SCHEDULER_OFF_REQ
			req.header.source = "%s:%s" %(self.node_info.info["nodeType"], self.node_info.info["nodeId"])
			req.header.dest = "scheduler"
			req.header.feedback = self.msg_cli.chn_name

			msg_data = json.dumps({
				"msg_type": req.header.type,
				"msg_data": base64.b64encode(req.SerializeToString())
			})
		else:
			pass

		if msg_data != "":
			self.sch_msg_cli.send_msg(msg_data)

	def exec_msg_cmd(self, cmd):
		op = cmd["op"]		
		if op == "view":
			# 读取消息通道
			msgs = self.msg_cli.get_msgs(-1)
			if msgs is not None and len(msgs) > 0:
				for msg in msgs:
					self.process_msg(msg)
			

	def process_msg(self, msg):
		if msg.header.type == msg_pb2.MAIL_ON_RSP:
			print "mail on account %s, returned %d, %s" %(msg.account, msg.code, msg.reason)
		elif msg.header.type == msg_pb2.MAIL_OFF_RSP:
			print "mail off account %s, returned %d, %s" %(msg.account, msg.code, msg.reason)
		elif msg.header.type == msg_pb2.MAIL_UPDATE_RSP:
			print "mail update account %s, returned %d, %s" %(msg.account, msg.code, msg.reason)
		elif msg.header.type == msg_pb2.MAIL_STATUS_RSP:
			print "mail status account %s, returned %d, %s; %s,%s,%s,%s,%d,%d" %(msg.account, msg.code, msg.reason,
				 msg.alias, msg.password, msg.smtpHost, msg.smtpPort, msg.useSsl, msg.status)
		elif msg.header.type == msg_pb2.JOB_ON_RSP:
			print "job on job %s, returned %d, %s" %(msg.jobId, msg.code, msg.reason)
		elif msg.header.type == msg_pb2.JOB_OFF_RSP:
			print "job off job %s, returned %d, %s" %(msg.jobId, msg.code, msg.reason)
		elif msg.header.type == msg_pb2.JOB_UPDATE_RSP:
			print "job update job %s, returned %d, %s" %(msg.jobId, msg.code, msg.reason)
		elif msg.header.type == msg_pb2.JOB_STATUS_RSP:
			print "job status job %s, returned %d, %s; %d, %d, %d" %(msg.jobId, msg.code, msg.reason, msg.completedNum, msg.successNum, msg.totalNum)
		elif msg.header.type == msg_pb2.JOB_LIST_RSP:
			print "job list {%s}, returned %d, %s" %(",".join(msg.jobList), msg.code, msg.reason)
		elif msg.header.type == msg_pb2.WORKER_LIST_RSP:
			pass
		elif msg.header.type == msg_pb2.WORKER_STATUS_RSP:
			pass
		else:
			pass

	def exec_help(self):
		print "    quit or exit"
		print "    msg {view}"
		print "    mail {on/off/update/status} -a account -A alias -p password -h smtp_host -P smtp_port -s security"
		print "    job {on/off/update/status/list} -j job_id -f job_file"
		print "    worker {list/status/off} -w worker_id"
		print "    scheduler {off}"


	def run(self):
		'''
		启动运行，并接受命令输入
		命令格式：
			quit
			exit
			mail {on/off/update/status} -a account -A alias -p password -h smtp_host -P smtp_port -s security
			job {on/off/update/status/list} -j job_id -f job_file
			worker {list/status} -w worker_id
			scheduler {off}
			msg view
		'''
		while True:
			cmdline = raw_input("%s-%s>> " %(self.node_info.info["nodeType"], self.node_info.info["nodeId"]))
			# 解析命令
			cmd = self.parse_cmd(cmdline)	
			if cmd is None:
				print "command %s does not support!" %(cmdline)
				continue
			if cmd["name"] == "quit" or cmd["name"] == "exit":
				print "Quit..."
				break
			elif cmd["name"] == "help":
				self.exec_help()	
			elif cmd["name"] == "mail":
				self.exec_mail_cmd(cmd)
			elif cmd["name"] == "job":
				self.exec_job_cmd(cmd)
			elif cmd["name"] == "worker":
				self.exec_worker_cmd(cmd)
			elif cmd["name"] == "msg":
				self.exec_msg_cmd(cmd)
			elif cmd["name"] == "scheduler":
				self.exec_scheduler_cmd(cmd)
			else:
				pass


if __name__ == "__main__":
	from config import global_config

	shell = MailShell(global_config)

	shell.run()

'''
'''
