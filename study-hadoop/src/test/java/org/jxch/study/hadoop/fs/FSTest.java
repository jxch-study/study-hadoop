package org.jxch.study.hadoop.fs;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 需要安装Hadoop程序, 并配置环境变量(HADOOP_HOME)
 * 不配置也可以使用部分功能
 */
@Slf4j
public class FSTest {

    public static FileSystem fileSystem;


    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        fileSystem = FileSystem.get(new URI("hdfs://192.168.140.130:9000"), new Configuration(), "root");
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    //显示根目录下文件
    @Test
    public void showRootFiles() throws Exception {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), false);
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            Path path = fileStatus.getPath();
            String name = path.getName();
            log.info("{} -> {}", path, name);
        }
    }

    //测试上传文件
    @Test
    public void testCopyFromLocalFile() throws Exception {
        Path src = new Path("E:\\work\\jxch-study\\study-hadoop\\study-hadoop\\src\\main\\resources\\test.txt");
        Path dst = new Path("/");
        fileSystem.copyFromLocalFile(src, dst);
    }

    //测试删除文件
    @Test
    public void testDelete() throws Exception {
        Path dst = new Path("/test.txt");
        fileSystem.delete(dst, true);
    }

    //测试使用流的方式上传
    @Test
    public void testUploadUseStream() throws Exception {
        FileInputStream fis = new FileInputStream("E:\\work\\jxch-study\\study-hadoop\\study-hadoop\\src\\main\\resources\\test.txt");
        Path path = new Path("/wc/input/test2.txt");
        FSDataOutputStream fos = fileSystem.create(path);
        IOUtils.copy(fis, fos);
    }

    //测试下载文件
    @Test
    public void testCopyToLocalFile() throws Exception {
//        这种方式只有配置了HADOOP_HOME之后才能使用
//        fileSystem.copyToLocalFile(new Path("/test.txt"), new Path("d:/"));

//        使用Java默认的方式 (RawLocalFileSystem)
        fileSystem.copyToLocalFile(false, new Path("/test.txt"), new Path("E:\\work\\jxch-study\\study-hadoop\\study-hadoop\\src\\main\\resources\\test2.txt"), true);
    }
}
