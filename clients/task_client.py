#!/usr/bin/env python
#-*- encoding: utf-8 -*-

'''
	purpose: 实现任务客户端，暂时只支持redis
	author : set_daemonr@126.com
	date   : 2017-06-22
	history: 
'''

import redis
import json

class Task(object):
	'''
	{
		"taskId": "",
		"jobId": "",
		"status": "",	
		"from": "",
		"to": "",
	}
	'''
	def __init__(self, **kw):
		self.info = {}
		self.set(**kw)

	def set(self, **kw):
		for k in kw.keys():
			self.info[k] = kw[k]

class TaskClient(object):
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

		self.scheme = TaskClient.Scheme(self.config["scheme"])
		self.chn_name = self.config["channel_name"]
		self.reconnect()

	def reconnect(self):
		if self.scheme.db_type == "redis":
			self.cli = redis.Redis(host = self.scheme.host, port = self.scheme.port, db = self.scheme.db)

	def query_job_tasks(self, job_id):
		'''
		查询job下的任务
		'''
		key = "job_tasks:%s" %(job_id)
		tasks = self.cli.smembers(key)

		return tasks

	def sync_job_task(self, job_id, task_id):
		'''
		增加job的task
		'''
		key = "job_tasks:%s" %(job_id)
		self.cli.sadd(key, task_id)

	def get_task(self, task_id):
		key = "task:%s" %(task_id)
		task_data = self.cli.hgetall(key)

		task = Task(**task_data)
		task.set(**{"taskId": task_id})

		return task

	def sync_task(self, **kw):
		key = "task:%s" %(kw["taskId"])
		self.cli.hmset(key, kw)

if __name__ == "__main__":
	#config = {
	#	"scheme": "redis://127.0.0.1:6379:3",
	#	"channel_name": "test_channel"
	#}
	from config import global_config
	cli = TaskClient(global_config["task_config"])
	job_id = "025a1414-5be6-11e7-bf3c-00163e0358f6"

	import sys
	for line in sys.stdin:
		line = line.strip('\n\t ')
		if line == "" or line[0] == '#':
			continue
		task_id = line
		cli.sync_job_task(job_id, task_id)
