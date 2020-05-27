package partflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PflowPartitioner  extends Partitioner<Text,Pflow> {
    @Override
    public int getPartition(Text text, Pflow pflow, int i) {
        if (pflow.getAddr().equals("bj"))
            return 0;
        else if(pflow.getAddr().equals("sh"))
        return 1;
        else   return 2;
    }
}
