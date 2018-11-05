package com.tomgs.hadoop.test.mapreduce.join.demo2;

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
import org.apache.orc.mapred.OrcStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

/**
 * @author tangzhongyuan
 * @create 2018-11-05 14:47
 **/
public class MainJob {

    private static Logger logger = LoggerFactory.getLogger(MainJob.class);

    public static class AppendMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            Map<String, Object> result = JsonUtil.convertJsonStrToMap(line);
            String resultKey = (String) result.get("table");
            String resultValue = "";
            if ((int)result.get("type") == 0) { //插入
                resultValue = "I" + result.get("I");
            } else if ((int)result.get("type") == 1) {
                resultValue = "U" + result.get("U");
            } else if ((int)result.get("type") == 2) {
                resultValue = "D" + result.get("D");
            }

            context.write(new Text(resultKey), new Text(resultValue));
        }
    }

    public static class OrcFileReadMapper extends Mapper<NullWritable, OrcStruct, Text, Text> {

        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {

            Map<String, Object> map = new HashMap<>();

            String resultKey = value.getFieldValue("table").toString();
            int columns = Integer.parseInt(value.getFieldValue("columns").toString());
            //int type = Integer.parseInt(value.getFieldValue("type").toString());
            String timeStamp = value.getFieldValue("timestamp").toString();
            String id = value.getFieldValue("id").toString();

            map.put("table", resultKey);
            map.put("columns", columns);
            map.put("timestamp", timeStamp);
            map.put("id", id);
            //map.put("type", type);

            for (int i = 0; i < columns; i++) {
                String filedName = "field" + i;
                String filedValue = value.getFieldValue(filedName).toString();
                map.put(filedName, filedValue);
            }

            String resultJson = JsonUtil.toJson(map);
            logger.info("resultJson: {}", resultJson);

            context.write(new Text(resultKey), new Text(resultJson));
        }
    }

    public static class MultiReducer extends Reducer<Text, Text, NullWritable, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Vector<String> insertData = new Vector<>();
            Vector<String> deleteData = new Vector<>();
            Vector<String> updateData = new Vector<>();
            Vector<String> originData = new Vector<>();

            for (Text value : values) {
                String dataValue = value.toString();
                if (dataValue.startsWith("I")) {
                    insertData.add(dataValue.substring(1));
                }
                if (dataValue.startsWith("U")) {
                    updateData.add(dataValue.substring(1));
                }
                if (dataValue.startsWith("D")) {
                    deleteData.add(dataValue.substring(1));
                }
                originData.add(dataValue);
            }

            //把新增数据插入到原始集合
            originData.addAll(insertData);
            //处理删除和更新
            for (Iterator<String> originIterator = originData.iterator(); originIterator.hasNext();) {
                String originDatum = originIterator.next();
                Map<String, Object> originDatumMap = JsonUtil.convertJsonStrToMap(originDatum);
                String id = String.valueOf(originDatumMap.get("id"));

                //删除处理
                for (Iterator<String> deleteIterator = deleteData.iterator(); deleteIterator.hasNext();) {
                    String deleteDatum = deleteIterator.next();
                    Map<String, Object> deleteDatumMap = JsonUtil.convertJsonStrToMap(deleteDatum);
                    if (!deleteDatumMap.containsKey("id")) {
                        continue;
                    }
                    String deleteId = String.valueOf(deleteDatumMap.get("id"));
                    if (deleteId.equals(id)) {
                        originIterator.remove();
                        deleteIterator.remove();
                        break;
                    }
                }

                //更新处理
                for (Iterator<String> updateIterator = updateData.iterator(); updateIterator.hasNext();) {
                    String updateDatum = updateIterator.next();
                    Map<String, Object> updateDatumMap = JsonUtil.convertJsonStrToMap(updateDatum);
                    if (!updateDatumMap.containsKey("id")) {
                        continue;
                    }
                    String updateId = String.valueOf(updateDatumMap.get("id"));
                    if (updateId.equals(id)) {
                        originDatumMap.putAll(updateDatumMap);
                        String updateJson = JsonUtil.toJson(originDatumMap);
                        originData.add(updateJson);
                        originIterator.remove();
                        break;
                    }
                }

            }
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

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MultiReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AppendMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, OrcFileReadMapper.class);

        Path outPath = new Path(args[2]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
    }

}
