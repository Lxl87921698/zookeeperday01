import cn.tedu.pojo.User;
import com.sun.xml.internal.txw2.output.DataWriter;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class SeriaDemo {

    @Test
    public void create(){
        User u1=new User();
        u1.setAge(18);
        u1.setGender("男");
        u1.setPassword("87921698");
        u1.setUsername("hahahah");
       // System.out.println(u1);

        User u2=new User("lxl","87921698","男",18);
        //System.out.println(u2);


        //方式三:适合于反射给值
        User u3=new User();
        u3.put("username","fafa");
        u3.put("password","87921698");
        u3.put("gender","male");
        u3.put("age",23);
        //System.out.println(u3);

        //方式4,使用于枚举给值
        User u4=new User();
        u4.put(0,"lxlx");
        u4.put(1,"5683475");
        u4.put(2,"female");
        u4.put(3,18);
        //System.out.println(u4);


        //方式5,建造者模式
        //创建一个和u4对象一样的对象,但是新用户的用户名和u4不一样
        User u5=User.newBuilder(u4).setUsername("发发发VB三个").build();
        System.out.println(u5);


    }


    @Test
    public void serial() throws IOException {
        //创建对象
        User u1=new User("法法","41241","男",18);
        User u2=new User("加工费","41241","男",18);
        User u3=new User("可以","41241","男",18);
        //创建序列化流
        DatumWriter<User> dw=new SpecificDatumWriter<>(User.class);
        //将序列化的数据写到文件中,需要创建文件流
        DataFileWriter<User> dfw=new DataFileWriter<>(dw);
        //指定文件
        dfw.create(u1.getSchema(),new File("D:\\a.txt"));
        //序列化对象
        dfw.append(u1);
        dfw.append(u2);
        dfw.append(u3);
        //关流
        dfw.close();
    }

    @Test
    public void deSerial() throws IOException {
        //创建反序列化流
        DatumReader<User> dr=new SpecificDatumReader<>(User.class);
        //从文件中反序列化,需要创建文件流
        DataFileReader<User> dfr=new DataFileReader<User>(new File("D:\\a.txt"),dr);
        //AVRO将DataFileReader设计成为了迭代器模式
        while (dfr.hasNext()){
            User u=dfr.next();
            System.out.println(u);
        }
        dfr.close();

    }



}
