package sortscore;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class scoreReducer extends Reducer<Text,Score,Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Score> values, Context context) throws IOException, InterruptedException {
         int avg=0;
        int avgg=0;
        for (Score sc:values) {

            int[] score = sc.getScore();
            int all=score.length;
            for (int i = 0; i <all ; i++) {
                avg+=score[i];
            }
             avgg=avg/all;

        }
        context.write(key,new IntWritable(avgg));
    }
}
