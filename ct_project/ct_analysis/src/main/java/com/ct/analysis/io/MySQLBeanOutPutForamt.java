package com.ct.analysis.io;

import com.ct.analysis.kv.AnalysisKey;
import com.ct.analysis.kv.AnalysisValue;
import com.ct.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
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
public class MySQLBeanOutPutForamt extends OutputFormat<AnalysisKey, AnalysisValue> {

    private FileOutputCommitter committer = null;

    protected static class MySQLRecordWriter extends RecordWriter<AnalysisKey, AnalysisValue> {

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
        public void write(AnalysisKey key, AnalysisValue value) throws IOException, InterruptedException {

            PreparedStatement preparedStatement = null;
            try {
                String insertSql = "insert into ct_call(teild,dateid,sumcall,sumduration) values(?,?,?,?)";
                preparedStatement = connection.prepareStatement(insertSql);

                preparedStatement.setInt(1, Integer.parseInt(key.getTel()));
                preparedStatement.setInt(2, Integer.parseInt(key.getDate()));
                preparedStatement.setInt(3, Integer.parseInt(value.getSumCall()));
                preparedStatement.setInt(4, Integer.parseInt(value.getSumDuration()));

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


    public RecordWriter<AnalysisKey, AnalysisValue> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
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
