package com.tomgs.hadoop.test.mapreduce.orcfile;

import com.tomgs.hadoop.test.mapreduce.orcfile.customer.CustomerRandomInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 用于map计数使用，在伪造数据的时候用到
 * @author tangzhongyuan
 * @create 2018-11-11 18:03
 **/
public class CustomerGenerateData {

    public static class AppendMapper extends Mapper<IntWritable, IntWritable, NullWritable, Text> {

        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

        @Override
        protected void map(IntWritable key, IntWritable value, Context context)
                throws IOException, InterruptedException {

            System.out.println("key:" + key);
            System.out.println("value" + value);

            //context.write(NullWritable.get(), new Text("key:" + key + ",value:" + value));
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            multipleOutputs.write(NullWritable.get(),new Text("key:" + key + ",value:" + value), fileName);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("tableNums", 10);
        conf.setInt("tableRows", 10);

        Job job = Job.getInstance(conf, "CustomerGenerateDataJob");
        job.setJarByClass(CustomerGenerateData.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: CustomerGenerateDataJob <in> [<in>...] <out>");
            System.exit(2);
        }

        job.setMapperClass(AppendMapper.class);

        job.setInputFormatClass(CustomerRandomInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(0);

        FileSystem fileSystem = FileSystem.get(conf);
        Path outpath = new Path(otherArgs[1]);
        if (fileSystem.exists(outpath)) {
            fileSystem.delete(outpath, true);
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, outpath);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
