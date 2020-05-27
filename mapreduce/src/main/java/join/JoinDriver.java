package join;

import mapreducelianxi.CountxDriver;
import mapreducelianxi.CountxMapper;
import mapreducelianxi.CountxReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class JoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(JoinDriver.class);
        job.setMapperClass(JoinMapper.class);
        //job.setReducerClass(.class);

        // Mapper和Reducer的输出类型一致
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //将关联的文件到缓存中,如果需要用到关联的文件,再从缓存中取出来处理
        URI []files={URI.create("hdfs://hadoop01:9000/txt/union/product.txt")};
        job.setCacheFiles(files);



        //按照要求,树出的文件中的格式 日期 / 利润
        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/txt/words.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/result/wordcount"));

        job.waitForCompletion(true);
    }
}
