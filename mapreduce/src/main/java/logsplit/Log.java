package logsplit;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Log implements WritableComparable<Log> {
    private  String ip="";
    private  String date="";
    private  String timeZone="";
    private  String way="";
    private  String http="";
    private  String version="";
    private  int status;

    public void setStatus(int status) {
        this.status = status;
    }

    private int num;

    @Override
    public String toString() {
        return "Log{" +
                "ip='" + ip + '\'' +
                ", date='" + date + '\'' +
                ", timeZone='" + timeZone + '\'' +
                ", way='" + way + '\'' +
                ", http='" + http + '\'' +
                ", version='" + version + '\'' +
                ", status='" + status + '\'' +
                ", num=" + num +
                '}';
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public String getWay() {
        return way;
    }

    public void setWay(String way) {
        this.way = way;
    }

    public String getHttp() {
        return http;
    }

    public void setHttp(String http) {
        this.http = http;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public int getStatus() {
        return status;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ip);
        dataOutput.writeUTF(date);
        dataOutput.writeUTF(timeZone);
        dataOutput.writeUTF(way);
        dataOutput.writeUTF(http);
        dataOutput.writeUTF(version);
        dataOutput.writeInt(status);
        dataOutput.writeInt(num);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
                 this.ip=dataInput.readUTF();
                 this.date=dataInput.readUTF();
                 this.timeZone=dataInput.readUTF();
                 this.way=dataInput.readUTF();
                 this.http=dataInput.readUTF();
                 this.version=dataInput.readUTF();
                 this.status=dataInput.readInt();
                 this.num=dataInput.readInt();
    }


    @Override
    public int compareTo(Log o) {
            return o.getStatus() -this.getStatus();
    }
}
