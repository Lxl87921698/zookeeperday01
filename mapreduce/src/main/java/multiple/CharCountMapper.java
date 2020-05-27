package multiple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CharCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            char[] arr = value.toString().toCharArray();

            for (char c:arr) {
            if(c!=' '){
                context.write(new Text(c+" "),new IntWritable(1));
            }

        }

    }
}
