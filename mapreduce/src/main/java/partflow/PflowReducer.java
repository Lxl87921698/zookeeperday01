package partflow;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PflowReducer extends Reducer<Text,Pflow,Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Pflow> values, Context context) throws IOException, InterruptedException {
        int  sum=0;
        for (Pflow f:values) {
            sum+=f.getFlow();

        }
        context.write(key,new IntWritable(sum));
    }
}
