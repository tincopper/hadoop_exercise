package com.tomgs.hadoop.test.mapreduce.join.demo5;

import com.tomgs.hadoop.test.mapreduce.customer.OrcCustomerOutputFormat;
import com.tomgs.hadoop.test.mapreduce.join.demo2.MainJob;
import com.tomgs.hadoop.test.mapreduce.join.demo4.JoinOrcJob2;
import com.tomgs.hadoop.test.util.JsonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Vector;

/**
 * @author tangzhongyuan
 * @create 2018-11-06 9:37
 **/
public class JoinOrcJob5 {
    private static Logger logger = LoggerFactory.getLogger(JoinOrcJob5.class);
    public static class MultiReducer extends Reducer<IntWritable, Text, NullWritable, OrcStruct> {

        private MultipleOutputs<NullWritable, OrcStruct> multipleOutputs;

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
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            if (values == null) {
                return;
            }

            Vector<String> insertData = new Vector<>();
            Vector<String> deleteData = new Vector<>();
            Vector<String> updateData = new Vector<>();
            Vector<String> originData = new Vector<>();

            JoinOrcJob2.MultiReducer.doCacheData(values, insertData, deleteData, updateData, originData);
            //把新增数据插入到原始集合
            originData.addAll(insertData);

            //要写入的内容
            Map<String, Object> firstMap = JsonUtil.convertJsonStrToMap(originData.get(0));

            //处理删除和更新
            if (deleteData.size() > 0 || updateData.size() > 0) {
                JoinOrcJob2.MultiReducer.doUpdateAndDelete(deleteData, updateData, originData);
            }

            int filedColumns = (int) firstMap.get("columns");

            TypeDescription schema = TypeDescription.createStruct();
            schema.addField("id", TypeDescription.createInt());
            schema.addField("columns", TypeDescription.createInt());
            schema.addField("table", TypeDescription.createInt());
            schema.addField("TS", TypeDescription.createString());
            for (int i = 0; i < filedColumns - 4; i++) {
                schema.addField("field" + i, TypeDescription.createString());
            }
            OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

            for (String originDatum : originData) {

                Map<String, Object> map = JsonUtil.convertJsonStrToMap(originDatum);
                int id = Integer.parseInt((String) map.get("id"));
                int columns = (int) map.get("columns");
                int table = (int) map.get("table");
                String timestamp = (String) map.get("TS");

                IntWritable idWritable = new IntWritable();
                idWritable.set(id);
                pair.setFieldValue(0, idWritable);

                IntWritable columnsWritable = new IntWritable();
                columnsWritable.set(columns);
                pair.setFieldValue(1, columnsWritable);

                IntWritable tableWritable = new IntWritable();
                tableWritable.set(table);
                pair.setFieldValue(2, tableWritable);

                Text timestampWritable = new Text();
                timestampWritable.set(timestamp);
                pair.setFieldValue(3, timestampWritable);

                for (int i = 0; i < map.size() - 4; i++) {
                    Text fieldWritable = new Text();
                    String filedValue = (String) map.get("field" + i);
                    fieldWritable.set(filedValue == null ? "" : filedValue);
                    pair.setFieldValue(i + 4, fieldWritable);
                }
                String resultPath = key.toString() + "/table_" + key.toString();
                multipleOutputs.write(NullWritable.get(), pair, resultPath);
            }

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 4) {
            throw new IllegalArgumentException("输入参数有误...");
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "demo5-join");
        job.setJarByClass(JoinOrcJob5.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MultiReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MainJob.AppendMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), OrcInputFormat.class, MainJob.OrcFileReadMapper.class);

        Path outPath = new Path(otherArgs[2]);
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

        System.exit(result ? 0 : 1);
    }
}
