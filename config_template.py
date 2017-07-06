#!/usr/bin/env python
#-*- encoding=utf-8 -*-

global_config = {
	# 中心调度器配置
	"scheduler": {
		"msg_channel": {
			"scheme": "redis://localhost:6379:1",
			"channel_name": ""
		},
		"logpath": "/tmp/ms_logs"
	},

	# worker配置
	"worker": {
		"msg_channel": {
			"scheme": "redis://localhost:6379:1",
			"channel_name": ""
		},
		"nodeId": "1",
		"logpath": "/tmp/ms_logs"
	},

	# job管理配置
	"job_config": {
		"scheme": "redis://localhost:6379:2",
		"channel_name": ""
	},

	# 任务管理配置
	"task_config": {
		"scheme": "redis://localhost:6379:3",	
		"channel_name": ""
	},

	# 发件箱
	"mails_config": {
		"scheme": "redis://localhost:6379:4",	
		"channel_name": ""
	},

	"shell_config": {
		"scheme": "redis://localhost:6379:1",	
		"channel_name": "shell"
	},

	"node_config": {
		"scheme": "redis://localhost:6379:1",	
		"channel_name": "node_info"
	},

	# 系统级别的汇报通道，暂时未实现
	"system": {
		"report_channels": {
			"mail": [
			]
		}
	}
}
