package com.hadoop.hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class WeiBoUtil {

    private static Configuration configuration = HBaseConfiguration.create();

    static {
        configuration.set("hbase.zookeeper.quorum","hadoop101");
    }

    /**
     * 创建命名空间
     * @param namespace
     * @throws IOException
     */
    public static void createNameSpace(String namespace) throws IOException {

        // 创建连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        // 创建NameSpace描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();

        // 创建操作
        admin.createNamespace(namespaceDescriptor);

        // 关闭资源
        admin.close();
        connection.close();

    }

    /**
     * 创建表
     * @param tableName 表名
     * @param versions 版本
     * @param cfs 列名
     * @throws IOException
     */
    public static void createTable(String tableName,int versions,String... cfs) throws IOException {

        // 创建连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        // 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 循坏添加列族
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setMaxVersions(versions);

            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        // 创建表
        admin.createTable(hTableDescriptor);

    }


    // 发布微博


    // 关注用户


    // 取关用户


    // 获取微博内容（初始化页面）


    // 获取微博内容（查看某个人所有微博内容）


}
