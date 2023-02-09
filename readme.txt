***************************************************
运行环境：
①Windows 11
②PyCharm Community Edition 2021.1 x64
***************************************************

1.ratf_node.py为运行ratf协议的节点实现，包含了完整的ratf协议流程实现，是整个项目的核心代码之一，由slaveServer.py程序调用以创建节点

2. “client1”“client2”文件夹代表两个不同物理位置的客户端，其目录下包含除端口配置数据外执行代码相同的client.py，以及不同的文本文件；

3. “group1”“group2”"group3"文件夹代表不同的集群，其下有除端口配置数据外执行代码相同的slaveServer.py，该代码运行从服务器程序；此外，包含的node*文件夹表示集群下的一个数据节点，每个数据节点存储集群下文件的一个副本；

4."mainServer"文件夹下仅包含mainServer.py，运行后执行主服务器程序

5.config.py为一些端口配置数据

6.log.py封装了ratf协议维持操作记录一致性的日志记录操作

7.ratf_node.py为运行ratf协议的节点实现，包含了完整的ratf协议流程实现，是整个项目的核心代码，又slaveServer.py程序调用以创建节点

8.rpc.py封装了常用的RPC操作

9.RWLock.py封装了读写锁的实现


