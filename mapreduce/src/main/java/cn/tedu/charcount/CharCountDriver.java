package cn.tedu.charcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CharCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 获取配置信息
        Configuration conf = new Configuration();
        // 先向YARN来申请任务用于执行MapReduce
        Job job = Job.getInstance(conf);

        // 设置入口类
        job.setJarByClass(CharCountDriver.class);
        // 设置Mapper类
        job.setMapperClass(CharCountMapper.class);
        // 设置Reducer类
        job.setReducerClass(CharCountReducer.class);

        // 设置Mapper的结果类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Reducer的结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //切片大小往小的调  改大的
        // FileInputFormat.setMaxInputSplitSize(job,545345);
        //切片大小往大的调   改小的
        //FileInputFormat.setMinInputSplitSize(job,3123123);

        // 指定输入路径
        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/txt/characters.txt"));
        // 指定输出路径
        // 要求输出的结果路径必须不存在
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/charcount"));

        // 提交任务
        job.waitForCompletion(true);
        //true 会监控打印日志
    }

}
