package com.tomgs.hadoop.test.mapreduce.join.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author tangzhongyuan
 * @create 2018-11-05 14:17
 **/
public class MainJob {

    public static class MultiMapper1 extends Mapper<LongWritable, Text, Text, FlagDataType> {

        private String delimiter;

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] arr = line.split(delimiter);
            if (2 == arr.length) {
                FlagDataType flagDataType = new FlagDataType();
                flagDataType.setInfo(arr[1].trim());
                flagDataType.setFlag(0);

                context.write(new Text(arr[0]), flagDataType);
            }
        }

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            delimiter = context.getConfiguration().get("delimiter", ",");
        }
    }

    public static class MultiMapper2 extends Mapper<LongWritable, Text, Text, FlagDataType> {

        private String delimiter;

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] arr = line.split(delimiter);
            if (2 == arr.length) {
                FlagDataType flagDataType = new FlagDataType();
                flagDataType.setInfo(arr[0].trim());
                flagDataType.setFlag(1);

                context.write(new Text(arr[1]), flagDataType);
            }
        }

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            delimiter = context.getConfiguration().get("delimiter", ",");
        }

    }

    public static class MultiReducer extends Reducer<Text, FlagDataType, NullWritable, Text> {

        private String delimiter;

        @Override
        protected void reduce(Text key, Iterable<FlagDataType> values, Context context)
                throws IOException, InterruptedException {
            String[] arr = new String[3];
            arr[2] = key.toString();
            for (FlagDataType flagDataType : values) {
                arr[flagDataType.getFlag()] = flagDataType.getInfo();
            }

            context.write(NullWritable.get(), new Text(arr[0] + delimiter + arr[1] + delimiter + arr[2]));
        }

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            delimiter = context.getConfiguration().get("delimiter", ",");
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("delimiter", ",");
        Job job = Job.getInstance(conf, "multi");
        job.setJarByClass(MainJob.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlagDataType.class);

        job.setReducerClass(MultiReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MultiMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MultiMapper2.class);

        Path outPath = new Path(args[2]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
    }
}
