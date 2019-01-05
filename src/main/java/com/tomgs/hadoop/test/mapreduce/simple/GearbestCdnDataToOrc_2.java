package com.tomgs.hadoop.test.mapreduce.simple;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
public class GearbestCdnDataToOrc_2 {
    private static Logger LOG = LoggerFactory.getLogger(GearbestCdnDataToOrc_2.class);

    private static String structInfo = "struct<id:string,message_reqhost:string,full_request:string,log_time:string,message_bytes:int," +
            "message_cliip:string,message_fwdhost:string,message_proto:string," +
            "message_reqmethod:string,message_reqpath:string,message_status:string," +
            "message_ua:string,netperf_asnum:int,netperf_downloadtime:int,netperf_edgeip:string," +
            "network_asnum:int,network_networktype:string,resphdr_setcookie:string,message_resplen:int," +
            "reqhdr_referer:string,biz_referer_2:string,message_respct:string,reqhdr_cookie:string," +
            "message_reqquery:string,resphdr_server:string,message_redirurl:string," +
            "message_reqlen:int,geo_city:string,geo_country:string,geo_lat:string,geo_long:string," +
            "geo_region:string,netperf_cachestatus:string>";

    public static class OrcWriterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

    public static class OrcWriterReducer extends Reducer<LongWritable, Text, NullWritable, OrcStruct> {

        private TypeDescription schema = TypeDescription.fromString(structInfo);

        private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

        private final NullWritable nada = NullWritable.get();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if (StringUtils.isBlank(value.toString())) {
                    return;
                }
                String[] arr = value.toString().split("\001");
                if (arr.length < 32) {
                    return;
                }
                for (int i = 0; i < arr.length; i++) {
                    Text strvalue = new Text();
                    IntWritable intvalue = new IntWritable();
                    if ((i == 4 || i == 12 || i == 13 || i == 15 || i == 18 || i == 26)) {

                        if (StringUtils.isBlank(arr[i])) {
                            intvalue.set(-1);
                        }
                        else if (StringUtils.isNumeric(arr[i])) {
                            intvalue.set(Integer.valueOf(arr[i]));
                        }
                        else {
                            LOG.error("illegal data content. data is not number, the content is [{}:{}:{}]", i, arr[i], value.toString());
                            /*throw new IllegalArgumentException("illegal data content. data is not number, " +
                                    "the content is [" + i + ":" + arr[i] + "] " + value.toString());*/
                            //这一行内容不合法，抛弃
                            System.err.println("illegal data content. data is not number, " +
                                    "the content is [" + i + ":" + arr[i] + ": " + value.toString() + "]");
                            return;
                        }
                        pair.setFieldValue(i, intvalue);
                    } else {
                        strvalue.set(arr[i]);
                        pair.setFieldValue(i, strvalue);
                    }

                }
                context.write(nada, pair);
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, structInfo);

        //conf.setBoolean("mapred.compress.map.out", true);//设置map输出压缩
        //conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);

        //conf.setBoolean("mapred.output.compress", true);//设置输出压缩

        String input = conf.get("task.input");
        String output = conf.get("task.output");

        Job job = Job.getInstance(conf, "orc job");
        job.setJarByClass(GearbestCdnDataToOrc_2.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: GearbestCdnDataToOrc <in> [<in>...] <out>");
            System.exit(2);
        }

        //String in = "hdfs://localhost:9000/user/work/warehouse/test/ddd.txt";
        //String out = "hdfs://localhost:9000/test/orc2";

        job.setMapperClass(OrcWriterMapper.class);
        job.setReducerClass(OrcWriterReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);

        job.setOutputFormatClass(OrcOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        OrcOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //FileInputFormat.addInputPath(job, new Path(input));
        //OrcOutputFormat.setOutputPath(job, new Path(output));

        OrcOutputFormat.setCompressOutput(job, true);
        OrcOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        long startTime = System.currentTimeMillis();
        boolean result = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();

        LOG.info("orc write cost time is :" + (endTime - startTime) + "ms.");

        System.exit(result ? 0 : 1);
    }

}
