package nofriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelationshipMapper extends Mapper<LongWritable, Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] person = value.toString().split(" ");

        context.write(new Text(person[0]),new Text(person[1]));

    }
}
