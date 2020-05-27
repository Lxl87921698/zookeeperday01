package sortscore;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ScoreMapper extends Mapper<LongWritable, Text,Text,Score> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split(" ");

        Score sco=new Score();
        sco.setName(arr[0]);
        int [] sc=new int[arr.length-1];
        for (int i = 1; i <arr.length ; i++) {
              sc[i-1]=Integer.parseInt(arr[i]);
        }
        sco.setScore(sc);

        context.write(new Text(sco.getName()),sco);
    }
}
