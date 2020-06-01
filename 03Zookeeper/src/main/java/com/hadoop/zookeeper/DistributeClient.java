package com.hadoop.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {
    private String connectionString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private int sessionTimeOut = 2000;
    private ZooKeeper zkClient;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeClient client = new DistributeClient();

        // 1 获取zookeeper集群连接
        client.getConnect();

        // 2 注册监听
        client.getChildren();

        // 3 业务逻辑
        client.business();

    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/servers", true);

        ArrayList<String> hosts = new ArrayList<>();

        for (String child : children) {
            byte[] data = zkClient.getData("/servers/" + child, false, null);

            hosts.add(new String(data));
        }

        // 将所有在线主机名称打印控制台
        System.out.println(hosts);

    }

    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectionString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    getChildren();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
