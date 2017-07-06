# pyems
a python coded Email Market System

## 动力来源
驱使自己将一个真实的案例进行开源，有两方面原因：1）用过很多开源的项目，却无任何贡献 2）这个案例的实现会让自己去更多思考平台化的设计和实现的问题，由此去探索hadoop的分布式文件系统、MR计算框架的设计，也很好奇于一个计算框架如何支持不同语言的执行、对资源的协调管理。

## EMS业务
EMS是Email Market System的缩写，即邮件营销系统。这种业务方式自邮箱出现以来就出现了，但是随着其它媒体渠道和形式的涌现，这种强烈广告性质且有潜在安全隐患的形式，其实对于我而言，并不是太看好，即使亚马逊推送的图书推荐我很喜欢，那也是因为亚马逊的品牌以及对图书类商品的关注。

要实施EMS业务，绕不开市面上常见的邮件服务系统，一般都会有垃圾邮件识别、访问控制措施，针对免费用户和付费用户也会提供差别式的服务，例如每天免费收发邮件数量、附件大小、发送频率等，根据测试，QQ邮箱（非会员、非付费）在连续发送200封邮件后，就开始被系统提示“超过最大限制”，这时换ip登录发送也不行，尝试过1～3个小时后再登录发送，有时仍会被限。
根据我的测试经验，用10个邮箱在4天成功发送了8000封邮件，与预期16000的发送量有差距，对邮件系统的规则还是没有深入了解。 所以这个可能是系统调度并没有达到最优的结果，但也说明一个问题，小打小闹的情况可以借助用免费工具、免费服务批量发送进行营销（前提要找服务器和ip资源），如果正儿八经的做营销，自己做平台的话，可以考虑和这些邮件服务商沟通，否则就需要找市面上的EMS服务商提供帮助。

要实现EMS的效果，与其它广告一样，
	1) 必须可视性好，如果邮件都被识别成垃圾邮件/危险邮件，用户看的机率极小，浪费一次信息传播机会；
	2) 内容要明确精炼，把自己的产品服务说清楚，公司或产品要有置信度（所以需要多渠道做准备），用户如果敢兴趣，一般会去搜索公司信息或者对比同类产品服务的优劣。
	3) 尽量少图、文字简练，标题正常，发件箱的名称以及发件箱要正规（参考了网上提供的资料）

## 功能
#### 单用户管理
非多用户服务场景
#### Job管理
#### 发件箱管理
#### 系统服务
worker服务关闭
scheduler服务关闭

## 架构
其实个人对系统架构理解并不深入，没办法从一个常规的架构思维体系中步步为营来构建系统的架构，只能从把握问题、设计业务的思路来不断修修改改，就像做衣服，我的理解好的系统架构师应该是一气呵成，不会产生过多的边角料，看来我还不是好的架构师，只能算是一个问题解决者。下面姑且称架构。

	整个系统包括worker、scheduler和shell，其中，shell提供系统管理的入口（意味着只能从命令行操作，还未设计web访问），scheduler实现调度，worker执行邮件的发送。

## 技术
实现这个系统的技术比较简单，python+redis，python编写业务，redis提供消息和数据服务，由于个人的状态，目前没考虑用zookeeper/mysql等服务来增强系统的可靠性，先简单实现用起来跑通逻辑、提供服务才是最紧要。

## 使用
	首先安装和启动redis。
	接下来，配置服务，将config_template.py修改为config.py，配置好worker、scheduler和shell的消息通道和节点配置等
	启动scheduler，执行bash start_scheduler.sh。
	启动worker，修改start_worker.sh脚本中的workers_host配置，里面以机器hostname为节点区分，当需要扩充worker节点时，在该配置后面添加新的即可。 执行bash start_worker.sh。
	启动shell，执行bash start_shell.sh，进入到命令行后，执行help可以看到相关命令的简要使用提示。

## 历史记录
