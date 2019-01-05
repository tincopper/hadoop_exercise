package com.tomgs.hadoop.test.mapreduce.join.demo7;

import com.tomgs.hadoop.test.mapreduce.join.demo4.JoinOrcJob2;
import com.tomgs.hadoop.test.util.JsonUtil;
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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
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
 * 
 * @author tomgs
 *
 */
public class JoinOrcJob7 {

    private static Logger logger = LoggerFactory.getLogger(JoinOrcJob7.class);

    public static class AppendMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            Map<String, Object> result = JsonUtil.convertJsonStrToMap(line);
            int tableId = (int) result.get("table");

            String data = result.get("data").toString();
            Map<String, Object> dataMap = JsonUtil.convertJsonStrToMap(data);

            String resultValue = "";
            String resultKey = tableId + "_" + dataMap.get("id");

            if ((int)result.get("type") == 0) { //插入
                resultValue = "I" + data;
            } else if ((int)result.get("type") == 1) {
                resultValue = "U" + data;
            } else if ((int)result.get("type") == 2) {
                resultValue = "D" + data;
            }
            context.write(new Text(resultKey), new Text(resultValue));
        }
    }

    public static class OrcFileReadMapper extends Mapper<NullWritable, OrcStruct, Text, Text> {

        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {

            Map<String, Object> map = new HashMap<>();

            String table = value.getFieldValue("table").toString();
            int tableId = Integer.parseInt(table);
            int columns = Integer.parseInt(value.getFieldValue("columns").toString());
            String timeStamp = value.getFieldValue("TS").toString();
            String id = value.getFieldValue("id").toString();

            map.put("table", tableId);
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

            context.write(new Text(table + "_" + id), new Text(resultJson));
        }
    }

    public static class MultiReducer2 extends Reducer<Text, Text, NullWritable, Text> {

        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            this.multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
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

            logger.info("inser data nums: {}", insertData.size());
            logger.info("delete data nums: {}", deleteData.size());
            logger.info("update data nums: {}", updateData.size());
            logger.info("origin data nums:{},", originData.size());

            //处理删除和更新
            if (deleteData.size() > 0 || updateData.size() > 0) {
                JoinOrcJob2.MultiReducer.doUpdateAndDelete(deleteData, updateData, originData);
            }

            for (String originDatum : originData) {

                Map<String, Object> map = JsonUtil.convertJsonStrToMap(originDatum);

                StringBuffer sb = new StringBuffer();
                sb.append(map.get("table")).append(",")
                        .append(map.get("columns")).append(",")
                        .append(map.get("TS")).append(",")
                        .append(map.get("id")).append(",");
                int cols = map.size() - 4;
                for (int i = 0; i < cols - 1; i++) {
                    sb.append(map.get("field" + i)).append(",");
                }
                sb.append(map.get("field" + (cols - 1)));

                String resultPath = "table_" + key.toString().split("_")[0];
                multipleOutputs.write(NullWritable.get(), new Text(sb.toString()), resultPath);
            }

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 3) {
            throw new IllegalArgumentException("输入参数有误...");
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "demo7-join");
        job.setJarByClass(JoinOrcJob7.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MultiReducer2.class);
        job.setNumReduceTasks(Integer.parseInt(otherArgs[3]));

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, AppendMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), OrcInputFormat.class, OrcFileReadMapper.class);

        Path outPath = new Path(otherArgs[2]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        job.setOutputFormatClass(TextOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outPath);

        long startTime = System.currentTimeMillis();
        boolean result = job.waitForCompletion(true);
        logger.info("cost time is : {}ms", System.currentTimeMillis() - startTime);

        System.exit(result ? 0 : 1);
    }
}
