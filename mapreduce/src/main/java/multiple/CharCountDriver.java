package multiple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class CharCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CharCountDriver.class);
        job.setMapperClass(CharCountMapper.class);
        job.setReducerClass(CharCountReducer.class);



        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);

        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop01:9000/txt/characters.txt"), TextInputFormat.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop01:9000/txt/words.txt"), TextInputFormat.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop01:9000/txt/invert/"), TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/multipleinput"));

        job.waitForCompletion(false);

    }
}
