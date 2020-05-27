package sortprofit;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProfitsReducer extends Reducer<Profits, NullWritable,Profits,NullWritable> {

    @Override
    protected void reduce(Profits key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
