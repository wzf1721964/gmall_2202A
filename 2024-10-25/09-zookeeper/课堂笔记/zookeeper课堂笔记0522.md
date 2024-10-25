> 课堂笔记

### 1、同步和异步

```
同步：干一件事情，在干另一件事情

异步：干一件事情，可以继续干另一件事，如果第一件事情完成以后，就通过接口回调给我
```

### 2、乐观锁和悲观锁

```
悲观锁：只能同时一个修改

乐观锁：可以同时多个人修改，但是只能有一个人成功，读出来的版本号和修改的版本号要保持一直
```

### 3、监听器原理

```
当创建Zookeeper对象的时候，会创建出两个守护线程，一个线程负责通信，一个线程负责当数据或者节点发送改变的时候，进行回调，会调用process这个方法。
```

```
 this.sendThread = new ClientCnxn.SendThread(clientCnxnSocket);
 this.eventThread = new ClientCnxn.EventThread();
```

### 4、ZAB协议

#### 4.1、选举

```
1--->3----follower
2--->3----follower
3--->3---leader
4--->3----follower
5--->3----follower

```

#### 4.2、如何判断是否厉害

```
先比较zxid,谁zxid大，谁就厉害，也就代表数据最新，如果zxid一样大，就比较myid，谁的myid大，谁就厉害
```

#### 4.3、写数据

```
如果每个不同写，就会自杀重启，然后同步数据。
```

#### 5、如何配置高可用（HA）

```
1、先拷贝一份hadoop源码到ha目录，把hadoop的data和logs文件删除掉

2、编辑core-site.xml

3、编辑hdfs-site.xml，并加上自动故障转移配置

4、然后同步ha目录到另外两台节点2

5、在一台节点上执行：hdfs namenode -format

6、在另外两台节点上分别执行：hdfs namenode -bootstrapStandby

7、初始化zk目录:hdfs zkfc -formatZK

8、环境先暂时换成ha目录

9、执行群起执行：start-dfs.sh
```

