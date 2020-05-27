package sortscore;

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

public class ScoreDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ScoreDriver.class);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(scoreReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Score.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/txt/score.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/serialscore"));

        job.waitForCompletion(true);
    }
}
