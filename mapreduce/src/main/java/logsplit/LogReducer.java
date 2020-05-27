package logsplit;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<Text, Log,Log,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Log> values, Context context) throws IOException, InterruptedException {
        for (Log x:values) {
            context.write(x,NullWritable.get());
        }
    }
}
