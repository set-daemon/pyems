#!/usr/bin/env python
#-*- encoding: utf-8 -*-

'''
	purpose: 实现发件箱访问客户端，暂时只支持redis
	author : set_daemonr@126.com
	date   : 2017-06-22
	history: 
'''

import redis
import json
import datetime

class MailAccount(object):
	def __init__(self, **kw):
		self.info = {}
		for k in kw.keys():
			self.info[k] = kw[k]

	def set(self, **kw):
		for k in kw.keys():
			self.info[k] = kw[k]

class MailAccountClient(object):
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

		self.scheme = MailAccountClient.Scheme(self.config["scheme"])
		self.chn_name = self.config["channel_name"]
		self.reconnect()

	def reconnect(self):
		if self.scheme.db_type == "redis":
			self.cli = redis.Redis(host = self.scheme.host, port = self.scheme.port, db = self.scheme.db)

	def get_account(self, account):
		key = "mail:%s" %(account)
		dict_data = self.cli.hgetall(key)
		account = MailAccount(**dict_data)

		return account

	def get_account_scheme(self, account):
		account_info = self.get_account(account)
		ssl = "ssl"
		if account_info.info["useSsl"] == "False":
			ssl = "non_ssl"

		account_scheme = "%s://%s/%s?user=%s&password=%s&aliasName=%s" %(
				ssl,
				account_info.info["smtpHost"],
				account_info.info["smtpPort"],
				account_info.info["account"],
				account_info.info["password"],
				account_info.info["alias"],
			)

		return account_scheme

	def sync_account(self, **kw):
		key = "mail:%s" %(kw["account"])
		self.cli.hmset(key, kw)

	def sync_account_worker(self, **kw):
		key = "mail_worker:%s:%s" %(kw["account"], kw["workerId"])		
		self.cli.hmset(key, kw)

	def query_account_worker(self, **kw):
		key = "mail_worker:%s:worker:%s" %(kw["account"], kw["workerId"])
		data = self.cli.hgetall(key)

		return data

	def query_account_worker_list(self, account):
		key = "mail_worker:%s*" %(account)
		worker_list = self.cli.keys(key)
		for i in range(0, len(worker_list)):
			worker_list[i] = worker_list[i].split(':')[-1]

		return worker_list

	def query_account_working_history(self, account):
		'''
		查询当前邮箱工作过的worker历史
		'''	
		worker_list = self.query_account_worker_list(account)	
		#print worker_list
		if worker_list is None or len(worker_list) <= 0:
			return None

		working_datas = {}
		for worker_id in worker_list:
			worker_id = worker_id.lstrip('worker:')
			account_worker_data = self.query_account_worker(**{
				"account": account,
				"workerId": worker_id
			})
			#print account_worker_data
			if account_worker_data is None or len(account_worker_data) <= 0:
				continue
			working_datas[worker_id] = account_worker_data

		return working_datas

	def query_account_selectable_workers(self, account, *workers):
		'''
		查询当前邮箱可以工作的worker列表
		'''
		selectable_workers = workers
		working_history = self.query_account_working_history(account)
		if working_history is None or len(working_history) <= 0:
			return selectable_workers

		now = datetime.datetime.now()
		for worker_id in working_history.keys():
			history_data = working_history[worker_id]
			if history_data["status"] == 1:
				continue
			if "lastFailTime" in history_data:
				t1 = datetime.datetime.strptime(history_data["lastFailTime"], "%Y-%m-%d %H:%M:%S")
				if (now - t1).total_seconds() > 60*60:
					# 一个小时后可以继续在该节点工作
					continue
			selectable_workers.remove(worker_id)

		return selectable_workers

	def check_account_ready(self, account):
		'''
		检查邮箱是否处于ready可工作状态
		会将处于发送间隔内、但是状态正常的邮箱过滤掉
		'''
		account_info = self.get_account(account)
		now = datetime.datetime.now()

		# 已下线
		if account_info.info["status"] == "-1":
			return False

		# 发送频率控制：150秒内不允许再发第二封邮件
		if "lastSendTime" in account_info.info:
			t1 = datetime.datetime.strptime(account_info.info["lastSendTime"], "%Y-%m-%d %H:%M:%S")
			if (now - t1).total_seconds() < 150:
				return False

		# 状态判断：只要发送间隔外即可
		if account_info.info["status"] == "1":
			return True

		# 上次失败后一个小时即可再次工作，
		# TODO：细化错误，不同错误恢复的时间不同
		if "lastFailTime" in account_info.info:
			last_fail_time = datetime.datetime.strptime(account_info.info["lastFailTime"], "%Y-%m-%d %H:%M:%S")
			if "failCode" in account_info.info:
				if account_info.info["failCode"] == "-4":
					return True

			if (now - last_fail_time).total_seconds() > 60*60:
				return True	

		return False

	def query_unworking_account_selectable_workers(self, in_accounts, workers):
		'''
		查询当前未工作的邮箱及其可部署worker列表
		应该还要去除已工作状态的邮箱占用的worker
		'''	
		accounts = in_accounts[:]
		# 已经被占用的列表
		occupied_workers = []
		onwork_accounts = self.query_all_working_accounts(*accounts)
		for account in onwork_accounts.keys():
			worker_id = onwork_accounts[account]
			occupied_workers.append(worker_id)
			accounts.remove(account)

		result = {}
		for account in accounts:
			if True != self.check_account_ready(account):
				continue

			working_history = self.query_account_working_history(account)
			if working_history is None or len(working_history) <= 0:
				result[account] = workers[:]
				continue

			can_not_work_on = []
			now = datetime.datetime.now()
			for worker_id in working_history.keys():
				history_data = working_history[worker_id]
				if history_data["status"] == 1:
					occupied_workers.append(worker_id)
					# 已经工作在该worker上，这种account需要被过滤掉
					can_not_work_on = workers[:]
					break
				if "lastFailTime" in history_data:
					t1 = datetime.datetime.strptime(history_data["lastFailTime"], "%Y-%m-%d %H:%M:%S")
					if (now - t1).total_seconds() < 60*60:
						# 一个小时后可以继续在该节点工作
						can_not_work_on.append(worker_id)
						continue

			can_work_on = list(set(workers) - set(can_not_work_on))
			if len(can_work_on) > 0:
				result[account] = can_work_on[:]

		# 将被占用的worker去除
		for account in result.keys():
			worker_list = result[account]
			can_work_on = list(set(worker_list) - set(occupied_workers))
			result[account] = can_work_on[:]

		return result

	def query_account_working_on(self, account):
		'''
		查询当前邮箱工作的worker
		'''
		history_workers = self.query_account_working_history(account)
		if history_workers is None:
			return None

		for worker_id in history_workers.keys():
			history_data = history_workers[worker_id]
			if history_data["status"] == "1":
				return worker_id

		return None

	def query_all_working_accounts(self, *accounts):
		'''
		查询所有工作中的邮箱
		'''
		working_accounts = {}
		for account in accounts:
			worker_id = self.query_account_working_on(account)
			if worker_id is None:
				continue
			working_accounts[account] = worker_id

		return working_accounts

	def query_all_available_accounts(self, *accounts):
		'''
 		查询所有可工作的邮箱列表
		'''
		available_accounts = []

		for account in accounts:
			if self.check_account_ready(account) == True:
				available_accounts.append(account)
				continue
				
		return available_accounts

if __name__ == "__main__":
	#config = {
	#	"scheme": "redis://127.0.0.1:6379:3",
	#	"channel_name": "test_channel"
	#}
	from config import global_config
	cli = MailAccountClient(global_config["mails_config"])
