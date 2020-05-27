package partflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import serialflow.Flow;
import serialflow.FlowDriver;
import serialflow.FlowMapper;
import serialflow.FlowReducer;

import java.io.IOException;

public class PflowDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(PflowDriver.class);
        job.setMapperClass(PflowMapper.class);
        job.setReducerClass(PflowReducer.class);
        job.setPartitionerClass(PflowPartitioner.class);
        job.setNumReduceTasks(3);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Pflow.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/txt/flow.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/serialflow"));

        job.waitForCompletion(true);
    }
}
