package lianxi03;

import cn.tedu.charcount.CharCountDriver;
import cn.tedu.charcount.CharCountMapper;
import cn.tedu.charcount.CharCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxScoreDriver {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        // 获取配置信息
        Configuration conf = new Configuration();
        // 先向YARN来申请任务用于执行MapReduce
        Job job = Job.getInstance(conf);

        // 设置入口类
        job.setJarByClass(MaxScoreDriver.class);
        // 设置Mapper类
        job.setMapperClass(MaxScoreMapper.class);
        // 设置Reducer类
        job.setReducerClass(MaxScoreReducer.class);



        // 设置Reducer的结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 指定输入路径
        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/txt/score2.txt"));
        // 指定输出路径
        // 要求输出的结果路径必须不存在
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/maxscore"));

        // 提交任务
        job.waitForCompletion(true);
    }
}
