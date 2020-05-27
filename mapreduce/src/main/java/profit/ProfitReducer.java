package profit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProfitReducer extends Reducer<Text,Profit,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Profit> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (Profit f:values) {
            sum+=(f.getR()-f.getC());
        }

        context.write(key,new IntWritable(sum));
    }
}
