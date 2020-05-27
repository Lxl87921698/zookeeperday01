package profit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ProfitDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ProfitDriver.class);
        job.setMapperClass(ProfitMapper.class);
        job.setReducerClass(ProfitReducer.class);

        //手动指定分区
        job.setPartitionerClass(ProfitPartitioner.class);
        //设置多少个分区
        job.setNumReduceTasks(5);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Profit.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/txt/profit.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/profit"));

        job.waitForCompletion(true);
    }
}
