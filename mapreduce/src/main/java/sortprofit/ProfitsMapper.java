package sortprofit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import profit.Profit;

import java.io.IOException;

public class ProfitsMapper extends Mapper<LongWritable, Text,Profits, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split(" ");

        Profits p=new Profits();
        p.setMonth(Integer.parseInt(arr[0]));
        p.setName(arr[1]);
        p.setProfit(Integer.parseInt(arr[2]));

        context.write(p,NullWritable.get());
    }
}
