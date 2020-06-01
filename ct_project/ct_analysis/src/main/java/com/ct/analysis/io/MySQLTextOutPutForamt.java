package com.ct.analysis.io;

import com.ct.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * MySQL数据格式化输入对象
 */
public class MySQLTextOutPutForamt extends OutputFormat {

    private FileOutputCommitter committer = null;

    protected static class MySQLRecordWriter extends RecordWriter<Text, Text> {

        private Connection connection = null;
        private Jedis jedis = null;


        public MySQLRecordWriter() {
            // 获取资源
            connection = JDBCUtil.getConnection();
            // 获取jedis
            jedis = new Jedis("hadoop102", 6379);
        }

        /**
         * 输出数据
         *
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        public void write(Text key, Text value) throws IOException, InterruptedException {

            String[] values = value.toString().split("_");
            String sumCall = values[0];
            String sumDuration = values[1];

            PreparedStatement preparedStatement = null;
            try {
                String insertSql = "insert into ct_call(teild,dateid,sumcall,sumduration) values(?,?,?,?)";
                preparedStatement = connection.prepareStatement(insertSql);


                String k = key.toString();
                String[] ks = k.split("_");

                String tel = ks[0];
                String date = ks[1];

                preparedStatement.setInt(1,Integer.parseInt(jedis.hget("ct_user",tel)));
                preparedStatement.setInt(2, Integer.parseInt(jedis.hget("ct_user",date)));
                preparedStatement.setInt(3, Integer.parseInt(sumCall));
                preparedStatement.setInt(4, Integer.parseInt(sumDuration));

                preparedStatement.executeUpdate();

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * 释放资源
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (jedis != null) {
                jedis.close();
            }
        }
    }


    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }

    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

}
