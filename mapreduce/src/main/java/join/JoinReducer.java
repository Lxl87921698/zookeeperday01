package join;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinReducer extends Reducer<Text,Order,Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<Order> values, Context context) throws IOException, InterruptedException {

        double sum=0;
        for (Order o:values) {

            sum+=o.getNum()*o.getPrice();
        }

        context.write(key,new DoubleWritable(sum));
    }
}
