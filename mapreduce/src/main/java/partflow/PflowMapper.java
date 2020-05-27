package partflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PflowMapper extends Mapper<LongWritable, Text,Text,Pflow> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split("");

        Pflow flow=new Pflow();
        flow.setPhone(arr[0]);
        flow.setAddr(arr[1]);
        flow.setName(arr[2]);
        flow.setFlow(Integer.parseInt(arr[3]));

        context.write(new Text(flow.getName()),flow);

    }
}
