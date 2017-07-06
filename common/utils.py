#!/usr/bin/env python
#-*- encoding: utf-8 -*-

def get_mails_from_file(file_name):
	'''
	从文件中读取邮箱，每行为一个邮箱
	'''
	mail_list = []
	with open(file_name, "r") as f:
		for line in f.readlines():
			mail_addr = line.strip('\n \t')
			if "@" not in mail_addr or mail_addr[0:1] == "#":
				continue
			mail_list.append(mail_addr)
	return mail_list

def get_mails(mail_list):
	'''
	从列表中读取邮箱，列表中的元素可能是邮箱文件
	'''
	ret_mail_list = []
	for m in mail_list:
		if "@" in m: # 简单判断是否邮箱格式
			ret_mail_list.append(m)
		else:
			f_mails = get_mails_from_file(m)
			if len(f_mails) > 0:
				ret_mail_list.extend(f_mails)
	return ret_mail_list
