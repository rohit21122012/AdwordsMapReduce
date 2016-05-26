package TwoLayer.CompYahooOurs;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by rohit on 5/19/16.
 */
public class MyGroupingComparater extends WritableComparator {
    public MyGroupingComparater() {
        super(MyMultiPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MyMultiPair p1 = (MyMultiPair) a;
        MyMultiPair p2 = (MyMultiPair) b;
        return p1.getKeyphrase().compareTo(p2.getKeyphrase());
    }
}
