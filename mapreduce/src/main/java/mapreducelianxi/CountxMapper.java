package mapreducelianxi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountxMapper extends Mapper<LongWritable, Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拆分单词
        // hello tom hello bob
        String[] arr = value.toString().split(" ");
        // 记录次数
        for (String s : arr) {
            context.write(new Text(s), new IntWritable(1));
        }

    }
}
