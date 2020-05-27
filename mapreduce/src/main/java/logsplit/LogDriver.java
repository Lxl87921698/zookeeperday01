package logsplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sortprofit.Profits;
import sortprofit.ProfitsDriver;
import sortprofit.ProfitsMapper;
import sortprofit.ProfitsReducer;

import java.io.IOException;

public class LogDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(LogDriver.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Log.class);




        job.setOutputKeyClass(Log.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/txt/log.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/log3"));

        job.waitForCompletion(true);
    }
}
