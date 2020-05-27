package profit;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Profit implements Writable {
    private String month="";
    private String name="";
    private Integer r;
    private Integer c;

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getR() {
        return r;
    }

    public void setR(Integer r) {
        this.r = r;
    }

    public Integer getC() {
        return c;
    }

    public void setC(Integer c) {
        this.c = c;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(month);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(r);
        dataOutput.writeInt(c);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
               this.month=dataInput.readUTF();
               this.name=dataInput.readUTF();
               this.r=dataInput.readInt();
               this.c=dataInput.readInt();

    }
}
