package nofriend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RelationshipReducer extends Reducer<Text,Text,Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> fs = new ArrayList<>();
        // 真实好友关系
        String name = key.toString();
        for (Text val : values) {
            String f = val.toString();
            fs.add(f);
            if (name.compareTo(f) < 0)
                context.write(new Text(name + "-" + f), new IntWritable(1));
            else
                context.write(new Text(f + "-" + name), new IntWritable(1));
        }
        // 隐藏好友
        for (int i = 0; i < fs.size() - 1; i++) {
            String f1 = fs.get(i);
            for (int j = i + 1; j < fs.size(); j++) {
                String f2 = fs.get(j);
                // 推测这俩好友之间是否认识
                if (f1.compareTo(f2) < 0)
                    context.write(new Text(f1 + "-" + f2), new IntWritable(0));
                else
                    context.write(new Text(f2 + "-" + f1), new IntWritable(0));

            }
        }

    }
}
