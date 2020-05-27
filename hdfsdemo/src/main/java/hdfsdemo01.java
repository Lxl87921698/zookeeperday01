import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class hdfsdemo01 {
    //上传文件
    @Test
    public void put() throws IOException, InterruptedException {
        //连接地址
        URI uri=URI.create("hdfs://10.42.130.77:9000");
        //配置信息
        Configuration conf=new Configuration();
        //连接HDFS
        FileSystem fs=FileSystem.get(uri,conf,"root");
        //指定上传的位置
        OutputStream out =fs.create(new Path("/b.txt"));
        //创建一个输入流
        FileInputStream in=new FileInputStream("D:\\a.txt");
        //上传文件
        IOUtils.copyBytes(in,out,conf);
        //关流
        in.close();
        out.close();
    }

    //下载
    @Test
    public void get() throws IOException {
        //连接地址
        URI uri= URI.create("hdfs://10.42.130.77:9000");
        //配置信息
        Configuration conf=new Configuration();
        //连接hdfs
        FileSystem fs=FileSystem.get(uri,conf);
        //指定要下载的文件
        InputStream in=fs.open(new Path("/b.log"));
        //创建输出流将数据写出
        FileOutputStream out=new FileOutputStream("D:\\lxl.log");
        //写出数据
        IOUtils.copyBytes(in,out,conf);
        //关流
        in.close();
        out.close();
    }
    //删除
    @Test
    public void delete() throws IOException, InterruptedException {
        //连接信息
        URI uri=URI.create("hdfs://10.42.130.77:9000");
        //配置信息
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(uri,conf,"root");
        //删除目录/文件
        //第二个参数表示是否递归删除
        fs.delete(new Path("/b.log"),true);

    }

    @Test
    public void put02(){
        URI uri =URI.create("hdfs://10.42.130.77:9000");
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(uri,conf);

        OutputStream out=fs.create("/log.xlx");
        FileInputStream in=new FileInputStream("D:\\lxl.txt");

        IOUtils.copyBytes(in,out,conf);

        in.close();
        out.close();



       }


}
