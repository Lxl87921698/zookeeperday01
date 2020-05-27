package homework01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class FileNameMapper extends Mapper<LongWritable, Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split(" ");
        FileSplit fs = (FileSplit) context.getInputSplit();
        String name = fs.getPath().getName();
        for (String s:arr) {
            context.write(new Text(s),new Text(name));
        }
    }
}
