package com.tomgs.hadoop.test.mapreduce.join.demo2;

import com.tomgs.hadoop.test.mapreduce.join.demo4.JoinOrcJob2;
import com.tomgs.hadoop.test.util.JsonUtil;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author tangzhongyuan
 * @create 2018-11-05 14:47
 **/
public class MainJob {

    private static Logger logger = LoggerFactory.getLogger(MainJob.class);

    public static class AppendMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            Map<String, Object> result = JsonUtil.convertJsonStrToMap(line);
            //String resultKey = (String) result.get("table");
            int intKey = (int) result.get("table");

            String resultValue = "";
            if ((int)result.get("type") == 0) { //插入
                resultValue = "I" + result.get("I");
            } else if ((int)result.get("type") == 1) {
                resultValue = "U" + result.get("U");
            } else if ((int)result.get("type") == 2) {
                resultValue = "D" + result.get("D");
            }
            context.write(new IntWritable(intKey), new Text(resultValue));
        }
    }

    public static class OrcFileReadMapper extends Mapper<NullWritable, OrcStruct, IntWritable, Text> {

        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {

            Map<String, Object> map = new HashMap<>();

            String resultKey = value.getFieldValue("table").toString();
            int intKey = Integer.parseInt(resultKey);
            int columns = Integer.parseInt(value.getFieldValue("columns").toString());
            String timeStamp = value.getFieldValue("TS").toString();
            String id = value.getFieldValue("id").toString();

            /*if (value.getFieldValue("type") != null) {
                int type = Integer.parseInt(value.getFieldValue("type").toString());
                map.put("type", type);
            }*/

            map.put("table", intKey);
            map.put("columns", columns);
            map.put("TS", timeStamp);
            map.put("id", id);

            for (int i = 0; i < columns - 4; i++) {
                String filedName = "field" + i;
                String filedValue = value.getFieldValue(filedName).toString();
                map.put(filedName, filedValue);
            }

            String resultJson = JsonUtil.toJson(map);
            //logger.info("resultJson: {}", resultJson);

            context.write(new IntWritable(intKey), new Text(resultJson));
        }
    }

    public static class MultiReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            if (values == null) {
                return;
            }

            List<String> insertData = new ArrayList<>();
            List<String> deleteData = new ArrayList<>();
            List<String> updateData = new ArrayList<>();
            List<String> originData = new ArrayList<>();

            JoinOrcJob2.MultiReducer.doCacheData(values, insertData, deleteData, updateData, originData);

            //把新增数据插入到原始集合
            originData.addAll(insertData);
            //处理删除和更新
            JoinOrcJob2.MultiReducer.doUpdateAndDelete(deleteData, updateData, originData);
            StringBuffer sb = new StringBuffer(1024 * 100);
            for (String originDatum : originData) {
                sb.append(originDatum).append("\n");
            }

            context.write(NullWritable.get(), new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "demo2-join");
        job.setJarByClass(MainJob.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MultiReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AppendMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), OrcInputFormat.class, OrcFileReadMapper.class);

        Path outPath = new Path(args[2]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        FileOutputFormat.setOutputPath(job, outPath);
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
