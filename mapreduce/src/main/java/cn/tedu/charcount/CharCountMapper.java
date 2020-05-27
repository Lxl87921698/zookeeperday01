package cn.tedu.charcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// 完成Map阶段
// MapReduce中要求被传输的数据能够被序列化
// 也因此MapReduce针对常用的类型提供了对应的序列化类
// KEYIN - 输入的键的类型 - 如果不指定，默认是行的字节偏移量
// VALUEIN - 输入的值的类型 - 如果不指定，默认是要处理的一行数据
// KEYOUT - 输出的键的类型 - 对于当前案例，输出的键是字符
// VALUEOUT - 输出的值的类型 - 对于当前案例，输出的值是次数
public class CharCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // 写Map的处理逻辑,一行数据执行一次map方法
    // key:输入的键 - 行的字节偏移量
    // value:输入的值 - 读取的一行数据
    // context:可以这个参数将数据从Map写到Reduce中
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拿到一行数据之后，需要先将这一行数据中的每一个字符拆分出来
        char[] cs = value.toString().toCharArray();
        // 拆分完成之后可以往外写
        // 假设现在这一行数据是hello，那么拆分出来的cs对应的就是{'h' , 'e', 'l' , 'l' , 'o'}
        // 统计方式就有2种：
        // 可以写出：h-1, e-1, l-2, o-1
        // 可以写出：h-1, e-1, l-1, l-1, o-1
        for (char c : cs) {
            context.write(new Text(c + ""), new IntWritable(1));
        }
    }

}
