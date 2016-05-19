/**
 * Created by rohit21122012 on 5/14/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RowNum {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "RowNum");
        job.setJarByClass(RowNum.class);

        job.setMapperClass(PhraseBidMapper.class);

        job.setReducerClass(DoubleMaxReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://127.0.0.1:9000/adsense/biddata"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://127.0.0.1:9000/adsense/withrow"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class PhraseBidMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    private static class DoubleMaxReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text row = values.iterator().next();
            context.write(key, row);
        }
    }
}
