package TwoLayer.CompYahooOurs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by rohit on 5/19/16.
 */
public class Driver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Driver");
        job.setJarByClass(Driver.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(MyGroupingComparater.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://127.0.0.1:9000/adsense/a3"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://127.0.0.1:9000/adsense/table"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
