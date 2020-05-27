package sortscore;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Score implements Writable {

    private  String name="";
    private  int score[]=new int[0];

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int[] getScore() {
        return score;
    }

    public void setScore(int[] score) {
        this.score = score;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
                    dataOutput.writeUTF(name);
                    dataOutput.writeInt(score.length);
        for (int sc:score) {
            dataOutput.writeInt(sc);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
                  this.name=dataInput.readUTF();
                  int len = dataInput.readInt();
                  this.score=new int[len];

        for (int i = 0; i <len ; i++) {
            score[i]=dataInput.readInt();
        }


    }
}
