package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {


    /**
     * HDFS文件上传
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testCopyFromLocalFile() throws IOException, URISyntaxException, InterruptedException {

        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");

        // 1 获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "hadoop");

        // 2 上传文件
        fs.copyFromLocalFile(new Path("e:/banzhang.txt"), new Path("/banzhang.txt"));

        // 3 关闭资源
        fs.close();

        System.out.println("over");
    }

    /**
     * HDFS文件下载
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "hadoop");

        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/banzhang.txt"), new Path("e:/banhua.txt"), true);

        // 3 关闭资源
        fs.close();
    }

    /**
     * HDFS文件刪除
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "hadoop");

        // 2 执行删除
        fs.delete(new Path("/0508/"), true);

        // 3 关闭资源
        fs.close();
    }

    /**
     * HDFS文件更名
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "hadoop");

        // 2 修改文件名称
        fs.rename(new Path("/banzhang.txt"), new Path("/banhua.txt"));

        // 3 关闭资源
        fs.close();
    }

    /**
     * HDFS文件详情
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException {

        // 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "hadoop");

        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();

            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("-----------分割线----------");
        }

        // 3 关闭资源
        fs.close();
    }

    /**
     * HDFS文件和文件夹的判断
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "hadoop");

        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }

}
