package TwoLayer.CompYahooOurs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by rohit on 5/19/16.
 */
public class MyMapper extends Mapper<LongWritable, Text, MyMultiPair, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        String[] keyPhraseParts = tokens[3].split(" ");
        Arrays.sort(keyPhraseParts);
        StringBuilder keyPhraseSorted = new StringBuilder();
        for (String part : keyPhraseParts) {
            keyPhraseSorted.append(part);
            keyPhraseSorted.append(" ");
        }
        MyMultiPair newMP = new MyMultiPair(tokens[0], tokens[1], tokens[2], keyPhraseSorted.toString());
        context.write(newMP, value);
    }
}
