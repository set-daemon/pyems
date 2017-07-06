#!/usr/bin/env python
#-*- encoding: utf-8 -*-

'''
	purpose: 实现job客户端，暂时只支持redis
	author : set_daemonr@126.com
	date   : 2017-06-22
	history: 
'''

import sys

import redis
import json

class Job(object):
	def __init__(self, **kw):
		self.info = {}
		for k in kw.keys():
			self.info[k] = kw[k]

	def set(self, **kw):
		for k in kw.keys():
			self.info[k] = kw[k]

class JobClient(object):
	'''
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

		self.scheme = JobClient.Scheme(self.config["scheme"])
		self.chn_name = self.config["channel_name"]
		self.reconnect()

	def reconnect(self):
		if self.scheme.db_type == "redis":
			self.cli = redis.Redis(host = self.scheme.host, port = self.scheme.port, db = self.scheme.db)

	def get_joblist(self):
		key = "job:*"
		job_list = self.cli.keys(key)

		# 去除前面的job:
		for i in range(0, len(job_list)):
			job_list[i] = job_list[i].lstrip('job:')
		return job_list

	def get_job(self, job_id):
		key = "job:%s" %(job_id)
		data = self.cli.hgetall(key)

		if len(data) <= 0:
			return None

		job = Job()
		job.set(**data)

		return job

	def get_maildata(self, job_id):
		key = "job:%s" %(job_id)
		mail_data = self.cli.hmget(key, ["mailData"])
		if len(mail_data) <= 0:
			return ""
	
		return mail_data[0]

	def sync_job(self, job):
		key = "job:%s" %(job.info["jobId"])
		self.cli.hmset(key, job.info)

	def update_sending_list(self, job_id, tos):
		'''
		支持第一次创建和执行过程中的更新
		'''
		key2 = "cque:%s" %(job_id)
		cque_senders = self.cli.smembers(key2)

		# 求差集
		to_update = list(set(tos) - set(cque_senders))

		key = "sque:%s" %(job_id)

		# 删除原来的数据
		self.cli.delete(key)

		# 注意：由于有些任务刚分配下去，所以可能会重复这些任务的发送

		tos_len = len(to_update)
		remain = tos_len
		seq = 0
		# 一次最大个数
		once_num = 100
		while remain > 0:
			fetch_num = once_num
			if remain <= once_num:
				fetch_num = remain

			end = seq + fetch_num
			self.cli.sadd(key, *to_update[seq:end])

			remain -= fetch_num
			seq += fetch_num

		return True

	def sending_done_once(self, job_id, done_account):
		key = "sque:%s" %(job_id)
		self.cli.srem(key, done_account)
		# 已完成队列
		key = "cque:%s" %(job_id)
		self.cli.sadd(key, done_account)

	def fetch_next_one(self, job_id):
		'''
		获取下一个待收件箱
		'''
		key = "sque:%s" %(job_id)
		to_account = self.cli.spop(key)

		return to_account

	def query_job_status(self, job_id):
		key1 = "sque:%s" %(job_id)
		sending_size = self.cli.scard(key1)
		key2 = "cque:%s" %(job_id)
		sent_size = self.cli.scard(key2)

		return {
			"sent": sent_size,
			"sending": sending_size,
			"total": sent_size + sending_size
		}

	def get_job_running_data(self, job_id):
		key = "job:%s" %(job_id)
		running_status = {}
		status_info = self.query_job_status(job_id)	
		for k in status_info.keys():
			running_status[k] = status_info[k]

		fields = ["owner", "senderAccounts", "status", "onTime", "jobId"]
		f_v = self.cli.hmget(key, fields)
		if f_v is None or len(f_v) <= 0:
			return running_status

		for i in range(0, len(fields)):
			field = fields[i]
			running_status[field] = f_v[i]

		return running_status

	def get_running_job(self):
		'''
			返回数据:
			{
				"jobId": "",
				"startTime": "",
				"status": 0/1
			}
		'''
		key = "job_onrunning"
		running_info = self.cli.hgetall(key)

		return running_info

	def sync_running_job(self, **kw):
		key = "job_onrunning"
		self.cli.hmset(key, kw)

	def query_suitable_jobs(self):
		'''
		取出适合调度的job
		'''
		jobs = {}
		job_list = self.get_joblist()
		for job_id in job_list:
			running_data = self.get_job_running_data(job_id)	
			if running_data["status"] == "0":
				# 过滤暂停执行的job
				continue
			# 检查是否都已经发送完毕
			if running_data["sent"] == running_data["total"]:
				continue

			jobs[job_id] = {}
			for k in running_data.keys():
				jobs[job_id][k] = running_data[k]

		return jobs

if __name__ == "__main__":
	#config = {
	#	"scheme": "redis://127.0.0.1:6379:3",
	#	"channel_name": "test_channel"
	#}
	from config import global_config
	cli = JobClient(global_config["job_config"])

	from mail_creator import MailCreator
	mail_creator = MailCreator()
	mail_creator.load(sys.argv[1])

	job = Job()
	job.job_id = "testjob_x0001"
	job.owner = "test_owner"
	job.from_mails = ["xxxxx@126.com"]
	job.to_mails = ["xxxxxx@qq.com"]
	job.mail_data = mail_creator.to_msg()

	cli.send_job(job)
