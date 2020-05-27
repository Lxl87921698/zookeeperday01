package join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Order  implements Writable {
    private String orderId="";
    private String date="";
    private String produteId="";
    private int num;
    private String name;
    private double price;

    public String getOderId() {
        return orderId;
    }

    public void setOderId(String oderId) {
        this.orderId = oderId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getProduteId() {
        return produteId;
    }

    public void setProduteId(String produteId) {
        this.produteId = produteId;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(date);
        dataOutput.writeUTF(produteId);
        dataOutput.writeInt(num);
        dataOutput.writeUTF(name);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
            this.orderId=dataInput.readUTF();
            this.date=dataInput.readUTF();
            this.produteId=dataInput.readUTF();
            this.num=dataInput.readInt();
            this.name=dataInput.readUTF();
            this.price=dataInput.readDouble();

    }
}
