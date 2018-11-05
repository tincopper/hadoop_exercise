package com.tomgs.hadoop.test.mapreduce.orcfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author tangzhongyuan
 * @create 2018-09-15 15:59
 **/
public class ORCJob {
    private static Logger LOG = LoggerFactory.getLogger(ORCJob.class);

    private static String structInfo = "struct<name:string,num:int>";

    public static class OrcWriterMapper extends Mapper<LongWritable, Text, NullWritable, OrcStruct> {

        private TypeDescription schema = TypeDescription.fromString(structInfo);

        private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

        private final NullWritable nada = NullWritable.get();
        private Text name = new Text();
        private IntWritable age = new IntWritable();

        public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

            if (!"".equals(value.toString())) {
                String[] arr = value.toString().split("\t");
                name.set(arr[0]);
                age.set(Integer.valueOf(arr[1]));
                pair.setFieldValue(0, name);
                pair.setFieldValue(1, age);
                output.write(nada, pair);
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, structInfo);

        Job job = Job.getInstance(conf, "orc job");
        job.setJarByClass(ORCJob.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: orcwrite <in> [<in>...] <out>");
            System.exit(2);
        }

        //String in = "hdfs://localhost:9000/user/work/warehouse/test/ddd.txt";
        //String out = "hdfs://localhost:9000/test/orc2";

        job.setMapperClass(OrcWriterMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(OrcOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

        OrcOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        long startTime = System.currentTimeMillis();
        boolean result = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();

        LOG.info("orc write cost time is :" + (endTime - startTime) + "ms.");

        System.exit(result ? 0 : 1);
    }

}
