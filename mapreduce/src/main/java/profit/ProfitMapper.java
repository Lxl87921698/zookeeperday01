package profit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProfitMapper  extends Mapper<LongWritable, Text,Text, Profit> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split(" ");
        Profit f=new Profit();
        f.setMonth(arr[0]);
        f.setName(arr[1]);
        f.setR(Integer.parseInt(arr[2]));
        f.setC(Integer.parseInt(arr[3]));
        context.write(new Text(f.getName()),f );
    }
}
