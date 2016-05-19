package CompYahooOurs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by rohit on 5/19/16.
 */
public class MyMultiPair implements Writable, WritableComparable<MyMultiPair> {
    private Text keyphrase = new Text();
    private IntWritable day = new IntWritable();
    private Text advertiserId = new Text();
    private IntWritable rank = new IntWritable();

    public MyMultiPair(String d, String adId, String r, String kp) {
        keyphrase.set(kp);
        day.set(Integer.valueOf(d));
        advertiserId.set(advertiserId);
        rank.set(Integer.valueOf(r));
    }

    
    @Override
    public int compareTo(MyMultiPair myMultiPair) {
//        Sort by keyword, then by day,  then by advertiser id, then by rank
        int compareValue = this.keyphrase.compareTo(myMultiPair.keyphrase);
        if (compareValue == 0) {
            compareValue = this.day.compareTo(myMultiPair.day);
            if (compareValue == 0) {
                compareValue = this.advertiserId.compareTo(myMultiPair.advertiserId);
                if (compareValue == 0) {
                    compareValue = this.rank.compareTo(myMultiPair.rank);
                } else {
                    return compareValue;
                }
            } else {
                return compareValue;
            }
        } else {
            return compareValue;
        }
        return compareValue;
    }

    public Text getKeyphrase() {
        return this.keyphrase;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        keyphrase.write(dataOutput);
        day.write(dataOutput);
        advertiserId.write(dataOutput);
        rank.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        keyphrase.readFields(dataInput);
        day.readFields(dataInput);
        advertiserId.readFields(dataInput);
        rank.readFields(dataInput);
    }
}
