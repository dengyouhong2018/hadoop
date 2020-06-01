package com.hadoop.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZookeeperTest {

    private String connectionString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private int sessionTimeOut = 2000;
    private ZooKeeper zkClient;

    @Before
    // 创建zookeeper客户端
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectionString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                List<String> children;
                try {
                    children = zkClient.getChildren("/", true);

                    for (String child : children) {
                        System.out.println(child);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });
    }

    @Test
    // 创建子节点
    public void createNode() throws KeeperException, InterruptedException {
        String path = zkClient.create("/zk", "study zookeeper".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    @Test
    // 判断zNode是否存在
    public void exists() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/zk", true);
        System.out.println(stat == null ? "not exist" : "exist");
    }

    @Test
    // 获取子节点并监听节点变化获取子节点并监听节点变化
    public void getDataAndWatch() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);

        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Long.MAX_VALUE);
    }

}
