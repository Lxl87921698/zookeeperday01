package cn.tedu.charcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
// import java.util.Iterator;

// 完成Reduce阶段
// KEYIN,VALUEIN - 输入的键值类型 - Reduce的数据是从Map来的
// 那么Map的输出类型就是Reduce的输入类型
// KEYOUT,VALUEOUT - 输出的简直类型 - 对于当前案例而言，输出的字符所对应的次数
public class CharCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // key:字符 - 每一个键调用一次reduce方法
    // values:这个字符对应的所有的次数 - 分组 - 分组过程不需要手动指定，在底层会自动根据键来分组
    // context:将数据写出到结果文件中
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // key = 'a'
        // values = 1,1,1,1,1,1...
        // 定义变量来记录总次数
        int sum = 0;
        // 遍历迭代器，求总次数
        for (IntWritable val : values) {
            sum += val.get();
        }
        // Iterator<IntWritable> it = values.iterator();
        // while(it.hasNext()){
        //     IntWritable val = it.next();
        //     sum += val.get();
        // }
        context.write(key, new IntWritable(sum));
}

}
