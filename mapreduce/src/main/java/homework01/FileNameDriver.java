package homework01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sortscore.Score;
import sortscore.ScoreDriver;
import sortscore.ScoreMapper;
import sortscore.scoreReducer;

import java.io.IOException;

public class FileNameDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FileNameDriver.class);
        job.setMapperClass(FileNameMapper.class);
        job.setReducerClass(FileNameReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/txt/invert/"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/nameFile"));

        job.waitForCompletion(true);

    }
}
