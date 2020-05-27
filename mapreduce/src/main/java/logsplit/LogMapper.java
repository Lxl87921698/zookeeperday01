package logsplit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text,Text,Log> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();

        String[] sp0 = s.split(" -- ");
        String[] sp1=sp0[1].split(" ");
        String[] sp11=sp1[0].split("\\[");
        String[] sp2 = sp1[1].split("\\]");
        String[] sp3 = sp1[2].split("\"");
        String[] sp4 = sp1[3].split("/");
        Log log=new Log();
        log.setIp(sp0[0]);
        log.setDate(sp11[1]);
        log.setTimeZone(sp2[0]);
        log.setWay(sp3[1]);
        log.setHttp(sp4[1]);
        log.setVersion(sp1[4]);
        log.setStatus(Integer.parseInt(sp1[5]));
        log.setNum(1);

      context.write(new Text(" "),log);
    }
}
