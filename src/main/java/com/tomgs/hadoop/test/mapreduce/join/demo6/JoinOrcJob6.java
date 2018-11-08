package com.tomgs.hadoop.test.mapreduce.join.demo6;

import com.tomgs.hadoop.test.mapreduce.customer.OrcCustomerOutputFormat;
import com.tomgs.hadoop.test.mapreduce.join.demo2.MainJob;
import com.tomgs.hadoop.test.mapreduce.join.demo5.JoinOrcJob5;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tangzhongyuan
 * @create 2018-11-08 15:58
 **/
public class JoinOrcJob6 extends Configured implements Tool {

    private static Logger logger = LoggerFactory.getLogger(JoinOrcJob6.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JoinOrcJob6(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException("输入参数有误...");
        }

        Configuration conf = new Configuration();
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "demo6-join");
        job.setJarByClass(JoinOrcJob5.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(JoinOrcJob5.MultiReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MainJob.AppendMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), OrcInputFormat.class, MainJob.OrcFileReadMapper.class);

        Path outPath = new Path(args[2]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        //job.setOutputFormatClass(OrcOutputFormat.class);不能丢，不然不能输出orc文件
        job.setOutputFormatClass(OrcCustomerOutputFormat.class);
        //OrcOutputFormat.setOutputPath(job, outPath);
        LazyOutputFormat.setOutputFormatClass(job, OrcCustomerOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outPath);

        long startTime = System.currentTimeMillis();
        boolean result = job.waitForCompletion(true);
        logger.info("cost time is : {}ms", System.currentTimeMillis() - startTime);

        return result ? 0 : 1;
    }
}
