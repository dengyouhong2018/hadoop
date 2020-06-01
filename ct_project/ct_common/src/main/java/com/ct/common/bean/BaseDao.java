package com.ct.common.bean;


import com.ct.common.annotation.Column;
import com.ct.common.annotation.Rowkey;
import com.ct.common.annotation.TableRef;
import com.ct.common.constant.Names;
import com.ct.common.constant.ValueConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * 基础数据访问对象
 */
public abstract class BaseDao {

    // HBase connection 对象连接池
    private ThreadLocal<Connection> connHolder = new ThreadLocal<>();

    // HBase admin 对象连接池
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<>();

    /**
     * 任务开始调用的方法
     *
     * @throws IOException IOException
     */
    protected void setup() throws IOException {
        getConnection();
    }

    /**
     * 任务结束调用的方法
     *
     * @throws IOException IOException
     */
    protected void cleanup() throws IOException {
        Admin admin = getAdmin();
        if (null != admin) {
            admin.close();
            adminHolder.remove();
        }

        Connection connection = getConnection();
        if (null != connection) {
            connection.close();
            connHolder.remove();
        }
    }

    /**
     * 获取Connection对象
     *
     * @return 返回connection对象
     */
    private synchronized Connection getConnection() throws IOException {
        Connection connection = connHolder.get();
        if (null == connection) {
            Configuration conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
            connHolder.set(connection);
        }
        return connection;
    }

    /**
     * 获取Admin对象
     *
     * @return 返回admin对象
     */
    private synchronized Admin getAdmin() throws IOException {
        Admin admin = adminHolder.get();
        if (null == admin) {
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    /**
     * 创建命名空间，如果命名空间已经存在，则不需要创建，否则创建新的
     *
     * @param namespace 命名空间名称
     */
    protected void createNamespaceNX(String namespace) throws IOException {
        Admin admin = getAdmin();
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
            // 获取失败后则创建命名空间
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * 创建表，如果表已存在，则删除后创建新的
     *
     * @param tableName 表名
     * @param families  列族
     * @throws IOException IOException
     */
    @SuppressWarnings("unused")
    protected void createTableXX(String tableName, String... families) throws IOException {
        createTableXX(tableName, null, families);
    }

    /**
     * 创建表，如果表已存在，则删除后创建新的
     *
     * @param tableName   表名
     * @param regionCount 分区数量
     * @param families    列族
     * @throws IOException IOException
     */
    protected void createTableXX(String tableName, Integer regionCount, String... families) throws IOException {
        Admin admin = getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            deleteTable(tableName);
        }
        createTable(tableName, regionCount, families);
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    protected void deleteTable(String tableName) throws IOException {
        Admin admin = getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param families  列族
     * @throws IOException IOException
     */
    private void createTable(String tableName, Integer regionCount, String... families) throws IOException {
        Admin admin = getAdmin();

        // 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 如果列族为空，则取默认值
        if (null == families || families.length == 0) {
            families = new String[1];
            families[0] = Names.CF_INFO.getValue();
        }

        // 循环添加列族
        for (String family : families) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        // 此处操作为增加预分区
        if (null == regionCount || regionCount <= 1) {
            admin.createTable(hTableDescriptor);
        } else {
            // 添加分区键
            byte[][] splitKeys = getSpliKeys(regionCount);
            admin.createTable(hTableDescriptor, splitKeys);
        }
    }

    /**
     * 生成分区键
     *
     * @param regionCount 分区数量
     * @return
     */
    private byte[][] getSpliKeys(Integer regionCount) {
        // 分区键数量
        int splitKeyCount = regionCount - 1;

        // 定义分区键
        byte[][] splitKeys = new byte[splitKeyCount][];

        ArrayList<byte[]> bsList = new ArrayList<>();
        for (int i = 0; i < splitKeyCount; i++) {
            String splitKey = i + "|";
            bsList.add(Bytes.toBytes(splitKey));
        }

        // 排序  HBase字节数组比较器
        //Collections.sort(bsList,new Bytes.ByteArrayComparator());

        bsList.toArray(splitKeys);

        return splitKeys;
    }

    /**
     * 增加数据
     *
     * @param tableName 表明
     * @param put       Put对象
     */
    @SuppressWarnings("unused")
    protected void putData(String tableName, Put put) throws IOException {
        // 获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(tableName));

        // 增加数据
        table.put(put);

        // 关闭表
        table.close();
    }

    /**
     * 增加多条数据
     *
     * @param tableName 表明
     * @param puts       Puts对象
     */
    @SuppressWarnings("unused")
    protected void putData(String tableName, List<Put> puts) throws IOException {
        // 获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(tableName));

        // 增加多条数据
        table.put(puts);

        // 关闭表
        table.close();
    }

    /**
     * 增加对象:自动封装数据，将对象数据直接保存到HBase中去
     *
     * @param obj put对象
     */
    protected void putObj(Object obj) throws IOException, IllegalAccessException {

        /**
         * 自定义注解，通过反射机制拿到表名
         */
        Class<?> clazz = obj.getClass();
        TableRef tableRef = clazz.getAnnotation(TableRef.class);
        String tableName = tableRef.value();

        /**
         * 自定义注解，通过反射机制拿到rowkey
         */
        Field[] fields = clazz.getDeclaredFields();
        String HBaseRowkey = "";
        for (Field field : fields) {
            Rowkey rowkey = field.getAnnotation(Rowkey.class);
            if (null != rowkey) {
                field.setAccessible(true);
                HBaseRowkey = (String) field.get(obj);
                break;
            }
        }

        // 获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(HBaseRowkey));

        /**
         * 自定义注解，通过反射机制拿到column和columnValue
         */
        for (Field field : fields) {
            Column column = field.getAnnotation(Column.class);
            if (null != column) {
                // 得到列族
                String family = column.family();
                // 得到列名，为空则取该对象的字段名
                String colName = column.column();
                if (null == colName || "".equals(colName)) {
                    colName = field.getName();
                }
                // 得到列名的值
                String value = (String) field.get(obj);

                // 添加列
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(colName), Bytes.toBytes(value));
            }
        }

        // 增加数据
        table.put(put);

        // 关闭表
        table.close();
    }

    /**
     * 计算分区号
     *
     * @param tel  电话号码
     * @param date 日期
     * @return
     */
    protected int genRegionNum(String tel, String date) {

        // 13112341234
        String userCode = tel.substring(tel.length() - 4);

        // 20190820225423
        String yearMonth = date.substring(0, 6);

        int userCodeHash = userCode.hashCode();
        int yearMothHash = yearMonth.hashCode();

        // crc校验采用异或算法，hash
        int crc = Math.abs(userCodeHash ^ yearMothHash);

        // 取模
        int regionNum = crc % ValueConstant.REGION_COUNT;

        return regionNum;
    }

    /**
     * 获取查询时startrow,stoprow集合
     *
     * @param tel
     * @param start
     * @param end
     * @return
     * @throws ParseException
     */
    protected List<String[]> getStartStopRowKeys(String tel, String start, String end) throws ParseException {
        List<String[]> rowKeys = new ArrayList<>();

        String startTime = start.substring(0, 6);
        String endTime = end.substring(0, 6);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");

        Calendar startCal = Calendar.getInstance();
        startCal.setTime(sdf.parse(startTime));

        Calendar endCal = Calendar.getInstance();
        endCal.setTime(sdf.parse(endTime));

        while (startCal.getTimeInMillis() <= endCal.getTimeInMillis()) {

            // 当前时间
            String nowTime = sdf.format(startCal.getTime());

            int regionNum = genRegionNum(tel, nowTime);

            String startRow = regionNum + "_" + tel + "_" + nowTime;
            String endRow = startRow + "|";

            String[] rowkey = {startRow, endRow};
            rowKeys.add(rowkey);

            //月份+1
            startCal.add(Calendar.MONTH, 1);
        }

        return rowKeys;
    }

//    public static void main(String[] args) throws ParseException {
//        List<String[]> rowKeys = getStartStopRowKeys("15616698676", "20180822", "20190222");
//
//        for (String[] rowKey : rowKeys) {
//            System.out.println(rowKey[0]+"~"+rowKey[1]);
//        }
//    }

}
