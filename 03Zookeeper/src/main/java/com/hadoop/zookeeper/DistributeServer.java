package com.hadoop.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributeServer {

    private String connectionString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private int sessionTimeOut = 2000;
    private ZooKeeper zkClient;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeServer server = new DistributeServer();

        // 1 连接zookeeper集群
        server.getConnect();

        // 2 注册节点
        server.regist(args[0]);

        // 3 业务逻辑
        server.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }



    private void regist(String hostname) throws KeeperException, InterruptedException {
        String path = zkClient.create("/servers/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(path + "--->" + hostname + " is on line" );
    }

    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectionString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }


}
