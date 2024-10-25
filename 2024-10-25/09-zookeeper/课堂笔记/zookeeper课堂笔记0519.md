> 课堂笔记

### 1、观察者模式

```
发布者：


订阅者/观察者：


Zookeeper:文件系统+通知机制
```

### 2、Zookeeper特点

```
Zookeeper只要有半数以上节点活着就可以正常对外提供服务
```

### 3、Zookeeper结构

```
跟Unix比较类似，每个节点都叫znode，每个znode都能存储1MB数据
```

### 4、集群搭建



```properties
0、编辑环境变量

#ZOOKEEPER_HOME
export ZOOKEEPER_HOME=/opt/module/zookeeper
export PATH=$PATH:$ZOOKEEPER_HOME/bin
```



```properties
1、把zoo_sample.cfg改成zoo.cfg


# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between
# sending a request and getting an acknowledgement
syncLimit=5
# the directory where the snapshot is stored.
# do not use /tmp for storage, /tmp here is just
# example sakes.
dataDir=/opt/module/zookeeper/zkData
# the port at which the clients will connect
clientPort=2181
#
# Be sure to read the maintenance section of the
# administrator guide before turning on autopurge.
#
# http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_maintenance
#
# The number of snapshots to retain in dataDir
#autopurge.snapRetainCount=3
# Purge task interval in hours
# Set to "0" to disable auto purge feature
#autopurge.purgeInterval=1
server.2=hadoop102:2888:3888
server.3=hadoop103:2888:3888
server.4=hadoop104:2888:3888

```

```
2、在zkData创建myid，每个节点值不一样，分别是2 3 4
```

```properties
3、常见命令
启动命令 
zkServer.sh start
查看状态
zkServer.sh status
关闭服务
zkServer.sh stop
```



```
练习：写一个zk启动脚本
```

### 5、常见命令

#### 5.1、查看节点

```
ls:查看子节点
```

```
ls2:查看子节点，并返回当前的节点额外信息 新版命令变成ls -s
```

```
ls 节点路径 watch:查看子节点，并把当前客户端注册到该节点当中，注册有效一次。新版命令变成ls 节点路径 -w
```

#### 5.2、创建节点

```
create:创建节点
	-e:临时,在临时节点下不运行创建子节点
	-s:有序

一共有四种节点类型：
    永久节点
    永久有序
    临时节点
    临时有序
```

#### 5.3、查看节点值和设置节点的值

```
get 节点路径：查看节点值 ，新版需用加上 get -s 才能获取到额外信息

get 节点路径 watch: 查看节点值，并注册到该节点当中，如果节点的值发生改变，则会通知

set 节点路径 值  ：给节点赋值
```

#### 5.4、删除节点

```
delete 节点路径：删除空节点

rmr 节点路径:删除非空节点
```

#### 5.5、查看节点状态

```
stat 节点路径：

czxid-创建节点的事务zxid
dataversion - znode数据变化号
dataLength- znode的数据长度
numChildren - znode子节点数量
ephemeralOwner：临时节点有值，值就是SessionID 非临时节点没有值，
```





### 6、练习题

```
a1 2
a2 3
a3 1
a1 1
a2 2
a3 0
a3 3
a4 1


输出结果
a1	1,2
a2	2,3
a3	0,1,3
a4	1
```

