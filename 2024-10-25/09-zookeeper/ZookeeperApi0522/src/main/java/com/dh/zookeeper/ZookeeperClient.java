package com.dh.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZookeeperClient {
    private String connect = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int timeOut = 10000;
    private ZooKeeper zooKeeper;

    //1、初始化一个对象
    //2、让对象帮咱们搞事情
    //3、释放对象


    @Before
    public void before() throws IOException {
        //1、对象
        zooKeeper = new ZooKeeper(connect, timeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
//                System.out.println("默认观察者");
            }
        });
    }


    // 创建节点
    @Test
    public void create() throws InterruptedException, KeeperException {
        //PERSISTENT 永久
        //PERSISTENT_SEQUENTIAL 永久有序

        //EPHEMERAL  临时
        //EPHEMERAL_SEQUENTIAL 临时有序
        // 2、创建节点
        zooKeeper.create("/test", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }


    // 查看子节点，并把自己当做观察者注册进去
    @Test
    public void ls() throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren("/test", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("节点发生变化了" + watchedEvent.toString());
            }
        });
        for (String child : children) {
            System.out.println(child);
        }
        // 模拟主线程别停
        Thread.sleep(Long.MAX_VALUE);
    }

    // 查看节点的值

    @Test
    public void getData() throws InterruptedException, KeeperException, IOException {
        Stat stat = new Stat();
        byte[] data = zooKeeper.getData("/test", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("数据发生改变了" + watchedEvent.toString());
            }
        }, stat);
        System.out.println(new String(data));
        System.out.write(data);
        System.out.println();
        System.out.println(stat.getDataLength());
        Thread.sleep(Long.MAX_VALUE);

    }

    // 修改值
    @Test
    public void setData() throws InterruptedException, KeeperException {
        Stat stat = zooKeeper.exists("/test", false);

        if (stat != null) {
            zooKeeper.setData("/test", "abc".getBytes(), stat.getVersion());
        }
    }

    //删除节点
    @Test
    public void deleteNode() throws InterruptedException, KeeperException {
//        Stat stat = zooKeeper.exists("/test2", false);
//        if (stat != null) {
//            zooKeeper.delete("/test2", stat.getVersion());
//        }
        deleteAll("/test");
    }

    public void deleteAll(String path) throws InterruptedException, KeeperException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat != null) {
            // 读出所有的子节点
            List<String> children = zooKeeper.getChildren(path, false);
            if (children.isEmpty()) {
                zooKeeper.delete(path, stat.getVersion());
            } else {
                //先删除子节点
                for (String child : children) {
                    deleteAll(path + "/" + child);
                }
                // 别忘删除自己
                zooKeeper.delete(path, stat.getVersion());
            }
        }
    }


    @After
    public void after() throws InterruptedException {
        zooKeeper.close();


    }
}
