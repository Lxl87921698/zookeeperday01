package profit;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProfitPartitioner extends Partitioner<Text, Profit> {

    @Override
    public int getPartition(Text text, Profit profit, int i) {
        String month=profit.getMonth();
        if(month.equals("1")){
            return 0;
        }else if (month.equals("2")){return 1;}
        else return 2;

    }
}
