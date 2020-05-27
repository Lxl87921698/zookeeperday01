package nofriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RelationshipDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(RelationshipDriver.class);
        job.setMapperClass(RelationshipMapper.class);
        job.setReducerClass(RelationshipReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
         //往小的调  改大的
       // FileInputFormat.setMaxInputSplitSize(job,545345);
        //往大的调   改小的
        //FileInputFormat.setMinInputSplitSize(job,3123123);

        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/txt/friend.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/relation2"));

        job.waitForCompletion(true);
        //true 会监控打印日志
    }
}
