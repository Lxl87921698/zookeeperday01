package homework01;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class FileNameReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String fileName="";
        Set<String> hashset=new HashSet();

        for (Text s:values) {
            //fileName=fileName+"\t"+s;
            hashset.add(s.toString());
        }
        for (String ss:hashset){
            fileName=fileName+"\t"+ss;
        }

        context.write(key,new Text(fileName));
    }
}
