package lianxi03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxScoreReducer  extends Reducer<Text, IntWritable,Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int max=0;
        for (IntWritable score:values) {
            if(max<score.get()){
                max=score.get();
            }
        }

        context.write(key,new IntWritable(max));
    }
}
