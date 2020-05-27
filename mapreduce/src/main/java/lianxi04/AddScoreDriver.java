package lianxi04;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AddScoreDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取配置信息
        Configuration conf = new Configuration();
        // 先向YARN来申请任务用于执行MapReduce
        Job job = Job.getInstance(conf);

        // 设置入口类
        job.setJarByClass(AddScoreDriver.class);
        // 设置Mapper类
        job.setMapperClass(AddScoreMapper.class);
        // 设置Reducer类
        job.setReducerClass(AddScoreReducer.class);



        // 设置Reducer的结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/txt/score2/"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/totalscore02"));

        // 提交任务
        job.waitForCompletion(true);
    }
}
