package mapreducelianxi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class CountxDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CountxDriver.class);
        job.setMapperClass(CountxMapper.class);
        job.setReducerClass(CountxReducer.class);

        // Mapper和Reducer的输出类型一致
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/txt/words.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/wordcount"));

        job.waitForCompletion(true);


    }
}
