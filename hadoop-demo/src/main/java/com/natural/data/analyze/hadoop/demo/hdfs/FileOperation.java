package com.natural.data.analyze.hadoop.demo.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class FileOperation {

    public static void main(String[] args) throws IOException, URISyntaxException {
        FileOperation fileOperation = new FileOperation();
        fileOperation.init();
    }

    public void init() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        // fileSystem 的地址 要查看 active http://192.168.56.106:9870/ 的那个 namenode 的 GUI overview 的情况
        // hdfs-core.xml 配置的， 默认端口是  8020
        // 配置 hadoop 集群的方法  1.
//        conf.set("fs.defaultFS", "hdfs://192.168.56.106:8020");
       // 配置 hadoop 集群的方法  2.
        URI uri = new URI("hdfs://192.168.56.106:8020");

        FileSystem fs = FileSystem.get(uri, conf);
        System.out.println("aaa");
        lsFile(fs, "/tmp");
        fs.close();
    }

    public void lsFile(FileSystem fs, String path) throws IOException {
        Path dst = new Path(path);
        FileStatus files[] = fs.listStatus(dst);

        for (FileStatus file : files) {
            System.out.println(file.getPath());
        }
    }

    /**
     * 上传文件
     * @param fs
     * @param filePath
     * @throws IOException
     */
    public void putFile(FileSystem fs, String filePath) throws IOException {

        Path src = new Path(filePath);
        Path dst = new Path("/test");
        fs.copyFromLocalFile(src, dst);
        System.out.println("Upload to hadoop" );

        FileStatus files[] = fs.listStatus(dst);

        for (FileStatus file : files) {
            System.out.println(file.getPath());
        }
    }
    // 创建目录
    public void mkdir(FileSystem fs, String path) throws IOException {
        Path dst = new Path(path);

        fs.mkdirs(dst);
    }
    // 删除目录和文件
    public void rmdir(FileSystem fs, String path) throws IOException {
        Path dst = new Path(path);
        fs.delete(dst, true);
    }

    // 下载文件
    public void getFile(FileSystem fs, String srcPath, String dstPath) throws IOException {
        Path src = new Path(srcPath);
        Path dst = new Path(dstPath);

        FSDataInputStream open = fs.open(src);

        FileOutputStream output = new FileOutputStream(dstPath);

        IOUtils.copy(open, output);

    }







}
