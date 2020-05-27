package mapreducerlianxi02;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DeleteSameDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf);

        job.setJarByClass(DeleteSameDriver.class);
        job.setMapperClass(DeleteSameMapper.class);
        job.setReducerClass(DeleteSameReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定输入路径
        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/txt/ip.txt"));
        // 指定输出路径
        // 要求输出的结果路径必须不存在
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/ipcount"));
        job.waitForCompletion(true);

    }
}
