package TwoLayer.CompYahooOurs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by rohit on 5/19/16.
 */
public class MyPartitioner extends Partitioner<MyMultiPair, Text> {
    @Override
    public int getPartition(MyMultiPair myMultiPair, Text text, int numberOfPartitions) {
        return Math.abs(myMultiPair.getKeyphrase().hashCode() % numberOfPartitions);
    }
}
