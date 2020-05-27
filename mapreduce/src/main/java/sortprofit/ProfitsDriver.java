package sortprofit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import serialflow.Flow;
import serialflow.FlowDriver;
import serialflow.FlowMapper;
import serialflow.FlowReducer;

import java.io.IOException;

public class ProfitsDriver  {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ProfitsDriver.class);
        job.setMapperClass(ProfitsMapper.class);
        job.setReducerClass(ProfitsReducer.class);

        job.setMapOutputKeyClass(Profits.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Profits.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/txt/profit2.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/profits07"));

        job.waitForCompletion(true);
    }
}
