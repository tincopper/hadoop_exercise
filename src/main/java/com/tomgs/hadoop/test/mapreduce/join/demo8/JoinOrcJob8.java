package com.tomgs.hadoop.test.mapreduce.join.demo8;

import com.tomgs.hadoop.test.mapreduce.customer.OrcCustomerInputFormat;
import com.tomgs.hadoop.test.mapreduce.customer.OrcCustomerOutputFormat;
import com.tomgs.hadoop.test.mapreduce.join.demo7.JoinOrcJob7;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 
 * @author tomgs
 *
 */
public class JoinOrcJob8 {

    private static Logger logger = LoggerFactory.getLogger(JoinOrcJob8.class);

    public static class OrcWriterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] arr = value.toString().split(",");
            int tableId = Integer.parseInt(arr[0]);

            context.write(new IntWritable(tableId), value);
        }
    }

    public static class OrcWriterReducer extends Reducer<IntWritable, Text, NullWritable, OrcStruct> {

        private MultipleOutputs<NullWritable, OrcStruct> multipleOutputs;

        @Override
        protected void setup(Context context) {
            this.multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            if (values == null) {
                return;
            }
            TypeDescription schema = null;
            OrcStruct pair = null;
            for (Text value : values) {
                String[] arr = value.toString().split(",");
                int tableId = Integer.parseInt(arr[0]);
                int filedColumns = Integer.parseInt(arr[1]);

                if (schema == null) {
                    schema = TypeDescription.createStruct();
                    schema.addField("table", TypeDescription.createInt());
                    schema.addField("columns", TypeDescription.createInt());
                    schema.addField("TS", TypeDescription.createString());
                    schema.addField("id", TypeDescription.createInt());
                    for (int i = 0; i < filedColumns - 4; i++) {
                        schema.addField("field" + i, TypeDescription.createString());
                    }
                    pair = (OrcStruct) OrcStruct.createValue(schema);
                }

                for (int i = 0; i < filedColumns; i++) {
                    Text strvalue = new Text();
                    IntWritable intvalue = new IntWritable();
                    if (i == 0 || i == 1 || i == 3) {
                        intvalue.set(Integer.parseInt(arr[i]));
                        pair.setFieldValue(i, intvalue);
                    } else {
                        strvalue.set(arr[i]);
                        pair.setFieldValue(i, strvalue);
                    }
                }
                String resultPath = "table_" + tableId;
                multipleOutputs.write(NullWritable.get(), pair, resultPath);
            }
        }
    }

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            throw new IllegalArgumentException("输入参数有误...");
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        //任务1
        Job job1 = Job.getInstance(conf, "demo8-join-job1");
        job1.setJarByClass(JoinOrcJob8.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(JoinOrcJob7.MultiReducer2.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(Integer.parseInt(otherArgs[4] == null ? "64" : otherArgs[4]));

        MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class, JoinOrcJob7.AppendMapper.class);
        MultipleInputs.addInputPath(job1, new Path(otherArgs[1]), OrcCustomerInputFormat.class, JoinOrcJob7.OrcFileReadMapper.class);

        Path outPath = new Path(otherArgs[2]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        job1.setOutputFormatClass(TextOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job1, outPath);

        //job1加入控制器
        ControlledJob ctrlJob1 = new ControlledJob(conf);
        ctrlJob1.setJob(job1);

        //任务2
        Job job2 = Job.getInstance(conf, "demo8-join-job2");
        job2.setJarByClass(JoinOrcJob8.class);
        job2.setMapperClass(OrcWriterMapper.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setNumReduceTasks(Integer.parseInt(otherArgs[5] == null ? "64" : otherArgs[5]));
        FileInputFormat.addInputPath(job2, outPath);

        job2.setReducerClass(OrcWriterReducer.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(OrcStruct.class);

        Path outPath2 = new Path(otherArgs[3]);
        FileSystem fs1 = FileSystem.get(conf);
        if (fs1.exists(outPath2)) {
            fs1.delete(outPath2, true);
        }

        job2.setOutputFormatClass(OrcCustomerOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job2, OrcCustomerOutputFormat.class);
        FileOutputFormat.setOutputPath(job2, outPath2);

        //job1加入控制器
        ControlledJob ctrlJob2 = new ControlledJob(conf);
        ctrlJob2.setJob(job2);

        //设置作业之间的以来关系，job2的输入以来job1的输出
        ctrlJob2.addDependingJob(ctrlJob1);

        //设置主控制器，控制job1和job2两个作业
        JobControl jobCtrl = new JobControl("demo8-join");
        //添加到总的JobControl里，进行控制
        jobCtrl.addJob(ctrlJob1);
        jobCtrl.addJob(ctrlJob2);

        //在线程中启动，记住一定要有这个
        Thread thread = new Thread(jobCtrl);
        thread.start();
        long startTime = System.currentTimeMillis();
        while (true) {
            if (jobCtrl.allFinished()) {
                logger.info(jobCtrl.getSuccessfulJobList().toString());
                jobCtrl.stop();
                break;
            }
        }
        logger.info("csot time : {}ms", System.currentTimeMillis() - startTime);
    }
}
