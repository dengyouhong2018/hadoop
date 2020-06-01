package com.project.cache;

import com.ct.common.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 使用redis缓存数据
 */
public class Bootstrap {
    public static void main(String[] args) {
        Map<String, Integer> userMap = new HashMap<String, Integer>();
        Map<String, Integer> dateMap = new HashMap<String, Integer>();

        // 读取用户，时间数据
        String queryUserSql = "select id,tel from ct_user";
        String queryDateSql = "select id,year,month,day from ct_date";

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = JDBCUtil.getConnection();
            preparedStatement = connection.prepareStatement(queryUserSql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Integer id = resultSet.getInt(1);
                String tel = resultSet.getString(2);
                userMap.put(tel, id);
            }

            preparedStatement = connection.prepareStatement(queryDateSql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Integer id = resultSet.getInt(1);
                String year = resultSet.getString(2);
                String month = resultSet.getString(3);
                if (month.length() == 1) {
                    month = "0" + month;
                }
                String day = resultSet.getString(4);
                if (day.length() == 1) {
                    day = "0" + month;
                }
                dateMap.put(year + month + day, id);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        // 先redis中存储数据
        Jedis jedis = new Jedis("hadoop102", 6379);

        Iterator<String> keyIterator = userMap.keySet().iterator();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            Integer value = userMap.get(key);
            jedis.hset("ct_user", key, value + "");
        }

        Iterator<String> dateIterator = dateMap.keySet().iterator();
        while (dateIterator.hasNext()) {
            String key = dateIterator.next();
            Integer value = dateMap.get(key);
            jedis.hset("ct_date", key, value + "");
        }

    }
}
