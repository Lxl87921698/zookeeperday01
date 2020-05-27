package sortprofit;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Profits implements Writable {
    private int month;
    private String name="";
    private int profit;

    @Override
    public String toString() {
        return "Profits{" +
                "month=" + month +
                ", name='" + name + '\'' +
                ", profit=" + profit +
                '}';
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getProfit() {
        return profit;
    }

    public void setProfit(int profit) {
        this.profit = profit;
    }

//    @Override
//    public int compareTo(Profits o) {
//        // 先按照月份升序
//        int r1 = this.getMonth() - o.getMonth();
//        if (r1 == 0) {
//            // 同一个月份中，按照利润降序
//            return o.getProfit() - this.getProfit();
//        }
//        return r1;
//    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(month);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(profit);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
             this.month= dataInput.readInt();
             this.name=dataInput.readUTF();
             this.profit=dataInput.readInt();
    }
}
