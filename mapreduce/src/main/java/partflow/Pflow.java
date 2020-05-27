package partflow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pflow implements Writable {

    private String phone;
    private String addr;
    private String name;
    private int flow;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getFlow() {
        return flow;
    }

    public void setFlow(int flow) {
        this.flow = flow;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phone);
        dataOutput.writeUTF(addr);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(flow);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
              this.phone=dataInput.readUTF();
              this.addr=dataInput.readUTF();
              this.name=dataInput.readUTF();
              this.flow=dataInput.readInt();
    }
}
