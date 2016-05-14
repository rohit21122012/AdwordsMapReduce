import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestMapReduce {
    //test comment
    private static class PhraseBidMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private LongWritable phraseIDKey = new LongWritable();
        private Text t = new Text();
        private DoubleWritable bidValue = new DoubleWritable();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int size,day,bid,impression,click,rank;
            String line = value.toString();
            String[] splitLine = line.split("\t");
            size = splitLine.length;
            //int[] keyphrase = new int[size-6];
            bid = Integer.parseInt(splitLine[4]);
            impression = Integer.parseInt(splitLine[5]);
            click = Integer.parseInt(splitLine[6]);
            rank = Integer.parseInt(splitLine[2]);
            for(int i = 0; i<size-6;i++){
                t.set(splitLine[i+2]);
                bidValue.set(1);
                context.write(t,bidValue);
            }

//            phraseIDKey.set(Long.parseLong(splitLine[1]));
//            bidValue.set(Double.parseDouble(splitLine[3]));
//            context.write(phraseIDKey, bidValue);
        }
    }

    private static class DoubleMaxReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable maxBidValue = new DoubleWritable();
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
            Double max = 0.0;
            maxBidValue.set(0);
            context.write(key, maxBidValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Test Map Reduce");
        job.setJarByClass(TestMapReduce.class);
        job.setMapperClass(PhraseBidMapper.class);
        job.setCombinerClass(DoubleMaxReducer.class);
        job.setReducerClass(DoubleMaxReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://127.0.0.1:9000/adsense/biddata"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://127.0.0.1:9000/adsense/maxbid"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
