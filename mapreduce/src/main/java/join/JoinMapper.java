package join;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class JoinMapper extends Mapper<LongWritable, Text,Text,Order> {

    //将product.txt在这个方法中先处理
    //map方法只需要从这个里面处理结果即可
     private Map<String,Order> map=new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {


        //先从缓存中取出来
        URI uri=context.getCacheFiles()[0];
        //连接hdfs
        FileSystem fs=FileSystem.get(uri,context.getConfiguration());
        //获取输入流
        InputStream in=fs.open(new Path(uri.toString()));
        //拿到输入流之后就可以读取这个文件了->输入流是一个字节流,但是product.txt中是一行一条数据

        //如果直接用这个字节流,那么还需要自己判断什么时候读取完一行 ->
          //所以考虑将这个字节流转换成一个字符流,最好能够按行读取
        LineReader reader=new LineReader(in);
        Text tmp=new Text();
        while (reader.readLine(tmp)!=0){
            String []arr=tmp.toString().split(" ");
            Order o=new Order();
            o.setProduteId(arr[0]);
            o.setName(arr[1]);
            o.setPrice(Double.parseDouble(arr[2]));
           map.put(o.getOderId(),o);
        }
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split(" ");

        Order o=new Order();
        o.setOderId(arr[0]);
        o.setDate(arr[1]);
        o.setProduteId(arr[2]);
        o.setNum(Integer.parseInt(arr[3]));
        o.setName(map.get(o.getProduteId()).getName());
        o.setPrice(map.get(o.getProduteId()).getPrice());
        context.write(new Text(o.getDate()),o);
    }
}
