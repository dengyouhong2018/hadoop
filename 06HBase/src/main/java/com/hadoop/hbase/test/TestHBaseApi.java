package com.hadoop.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TestHBaseApi {

    private static Configuration conf = null;
    private static Admin admin = null;
    private static Connection connection = null;

    static {
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop101");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 关闭资源
     *
     * @param connection
     * @param admin
     */
    public static void close(Connection connection, Admin admin) {

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static Logger getLogger() {
        Logger logger = Logger.getLogger(TestHBaseApi.class);
        return logger;
    }

    // 判断表是否存在
    public static boolean tableExits(String tableName) throws IOException {

        // Hbase配置文件
        Configuration conf = HBaseConfiguration.create();

        //HBaseConfiguration conf = new HBaseConfiguration();
        conf.set("hbase.zookeeper.quorum", "hadoop101"); // 主机名称
        conf.set("hbase.zookeeper.property.clientPort", "2181"); // 端口号

        // 获取Hbase管理员对象(老API)
        // HBaseAdmin admin = new HBaseAdmin(conf);

        // 获取Hbase管理员对象(新API)
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin1 = connection.getAdmin();

        // 执行
        //boolean flag = admin.tableExists(tableName);
        boolean flag = admin1.tableExists(TableName.valueOf(tableName));

        // 关闭资源
        //admin.close();
        admin1.close();

        return flag;
    }

    // 创建表
    public static void createTable(String tableName, String... cfs) throws IOException {

        // 先判断表是否存在
        if (tableExits(tableName)) {
            Logger.getLogger(TestHBaseApi.class).info("表已存在...");
            return;
        }

        // 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 添加列族
        for (String cf : cfs) {
            // 创建列描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        // 创建表操作
        admin.createTable(hTableDescriptor);
    }

    // 删除表
    public static void dropTable(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        Admin admin = connection.getAdmin();
        if (tableExits(tableName)) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！");
        }
    }


    // 增、改相同
    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {

        Table hTable = connection.getTable(TableName.valueOf(tableName));

        // 创建HTable对象
        // HTable hTable = new HTable(conf, TableName.valueOf(tableName));

        // 向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));

        // 向put对象中组装数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));

        hTable.put(put);

        hTable.close();

        Logger.getLogger(TestHBaseApi.class).info("插入数据成功。。。");

        // 删除数据
        // Delete delete = new Delete(Bytes.toBytes("sex"));
        // hTable.delete(delete);

    }


    // 查（分两种：全表扫描，指定列族）
    // 1 、全表扫描
    public static void scanTable(String tableName) throws IOException {

        Table hTable = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        //这里还可以指定startRow 和 stopRow
        //scan.setStartRow();
        //scan.setStopRow();

        ResultScanner results = hTable.getScanner(scan);

        // 遍历数据并打印
        for (Result result : results) {
            Cell[] cells = result.rawCells();

            for (Cell cell : cells) {
                getLogger().info(
                        "----> RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +    //得到rowkey
                                ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +      //得到列族
                                ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +   //得到列名
                                ",value:" + Bytes.toString(CellUtil.cloneValue(cell))      //得到值
                );
            }
        }

        hTable.close();
    }

    // 2、获取指定列族：列的数据
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {

        //获取talbe对象
        Table hTable = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));

        Result result = hTable.get(get);

        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {
            getLogger().info(
                    "----> RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +    //得到rowkey
                            ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +      //得到列族
                            ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +   //得到列名
                            ",value:" + Bytes.toString(CellUtil.cloneValue(cell))      //得到值
            );
        }

        hTable.close();
    }


    public static void main(String[] args) throws IOException {
//        System.out.println(tableExits("student"));
//        System.out.println(tableExits("staff"));

//        createTable("staff", "info");
//        System.out.println(tableExits("staff"));

//        addRowData("staff","1001","info","name","tom");

        scanTable("student");

        close(connection, admin);

    }
}
