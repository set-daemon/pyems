#!/usr/bin/env python
#-*- encoding: utf-8 -*-

'''
	purpose: 定义消息结构
	author : set_daemonr@126.com
	date   : 2017-06-25
	history: 
'''

import sys
sys.path.insert(0, "../")
sys.path.insert(0, "./")
reload(sys)

import json
import base64

from proto import msg_pb2

class MsgResolve(object):
	def __init__(self, **kw):
		pass

	@staticmethod
	def parse(msg_data):
		json_obj = json.loads(msg_data)
		if "msg_type" not in json_obj or "msg_data" not in json_obj:
			return None
		msg_type = json_obj["msg_type"]
		msg_data = base64.b64decode(json_obj["msg_data"])
		#print msg_type
		#print "%d\n%s" %(len(msg_data), msg_data)
		if msg_type == msg_pb2.MAIL_ON_REQ:
			proto_obj = msg_pb2.MailOnReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.MAIL_ON_RSP:
			proto_obj = msg_pb2.MailOnRsp()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.MAIL_OFF_REQ:
			proto_obj = msg_pb2.MailOffReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.MAIL_OFF_RSP:
			proto_obj = msg_pb2.MailOffRsp()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.MAIL_UPDATE_REQ:
			proto_obj = msg_pb2.MailUpdateReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.MAIL_UPDATE_RSP:
			proto_obj = msg_pb2.MailUpdateRsp()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.MAIL_STATUS_REQ:
			proto_obj = msg_pb2.MailStatusReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.MAIL_STATUS_RSP:
			proto_obj = msg_pb2.MailStatusRsp()
			proto_obj.ParseFromString(msg_data)

		elif msg_type == msg_pb2.JOB_ON_REQ:
			proto_obj = msg_pb2.JobOnReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.JOB_ON_RSP:
			proto_obj = msg_pb2.JobOnRsp()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.JOB_OFF_REQ:
			proto_obj = msg_pb2.JobOffReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.JOB_OFF_RSP:
			proto_obj = msg_pb2.JobOffRsp()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.JOB_UPDATE_REQ:
			proto_obj = msg_pb2.JobUpdateReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.JOB_UPDATE_RSP:
			proto_obj = msg_pb2.JobUpdateRsp()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.JOB_STATUS_REQ:
			proto_obj = msg_pb2.JobStatusReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.JOB_STATUS_RSP:
			proto_obj = msg_pb2.JobStatusRsp()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.JOB_LIST_REQ:
			proto_obj = msg_pb2.JobListReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.JOB_LIST_RSP:
			proto_obj = msg_pb2.JobListRsp()
			proto_obj.ParseFromString(msg_data)

		elif msg_type == msg_pb2.WORKER_LIST_REQ:
			proto_obj = msg_pb2.WorkerListReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.WORKER_LIST_RSP:
			proto_obj = msg_pb2.WorkerListRsp()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.WORKER_STATUS_REQ:
			proto_obj = msg_pb2.WorkerStatusReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.WORKER_STATUS_RSP:
			proto_obj = msg_pb2.WorkerStatusRsp()
			proto_obj.ParseFromString(msg_data)

		elif msg_type == msg_pb2.MAIL_ALTER_REQ:
			proto_obj = msg_pb2.MailAlterReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.MAIL_ALTER_RSP:
			proto_obj = msg_pb2.MailAlterRsp()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.TASK_ASSIGN_REQ:
			proto_obj = msg_pb2.TaskAssignReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.TASK_ASSIGN_RSP:
			proto_obj = msg_pb2.TaskAssignRsp()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.SCHEDULER_OFF_REQ:
			proto_obj = msg_pb2.SchedulerOffReq()
			proto_obj.ParseFromString(msg_data)
		elif msg_type == msg_pb2.WORKER_OFF_REQ:
			proto_obj = msg_pb2.WorkerOffReq()
			proto_obj.ParseFromString(msg_data)

		else:
			pass

		return proto_obj
