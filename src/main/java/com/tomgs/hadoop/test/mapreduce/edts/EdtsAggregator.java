package com.tomgs.hadoop.test.mapreduce.join.demo9;

import com.tomgs.hadoop.test.mapreduce.join.demo4.JoinOrcJob2;
import com.tomgs.hadoop.test.mapreduce.join.demo7.JoinOrcJob7;
import com.tomgs.hadoop.test.mapreduce.orcfile.customer.CustomerRandomOutputFormat;
import com.tomgs.hadoop.test.util.JsonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 这个测试主要用于将读取的文件进行join之到写入到文本的性能测试。<br/>
 * 这个在500张表每个表100w行数据的情况下，配置500个reduce的情况下大约需要花费13min左右
 *
 * @author tangzhongyuan
 * @create 2018-11-14 10:14
 **/
public class JoinTxtJob9 {

    private static Logger logger = LoggerFactory.getLogger(JoinTxtJob9.class);

    public static class MultiReducer extends Reducer<Text, Text, NullWritable, Text> {

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

            StringBuffer sb = new StringBuffer();
            String resultPath = "table_" + key.toString().split("_")[0];
            for (String originDatum : originData) {

                Map<String, Object> map = JsonUtil.convertJsonStrToMap(originDatum);

                sb.append(map.get("table")).append(",")
                        .append(map.get("columns")).append(",")
                        .append(map.get("TS")).append(",")
                        .append(map.get("id")).append(",");
                int cols = map.size() - 4;
                for (int i = 0; i < cols - 1; i++) {
                    sb.append(map.get("field" + i)).append(",");
                }
                sb.append(map.get("field" + (cols - 1)));
                if (sb.capacity() > 1024 * 1024) {
                    multipleOutputs.write(NullWritable.get(), new Text(sb.toString()), resultPath);
                    sb.delete(0, sb.length());
                }
            }
            if (sb.length() <= 0) {
                return;
            }

            multipleOutputs.write(NullWritable.get(), new Text(sb.toString()), resultPath);
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 3) {
            throw new IllegalArgumentException("输入参数有误...");
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "demo9-join-" + otherArgs[4]);
        job.setJarByClass(JoinOrcJob7.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MultiReducer.class);
        job.setPartitionerClass(JoinJob9Partitioner.class);
        job.setNumReduceTasks(Integer.parseInt(otherArgs[3]));

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, JoinOrcJob7.AppendMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), OrcInputFormat.class, JoinOrcJob7.OrcFileReadMapper.class);

        Path outPath = new Path(otherArgs[2]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        job.setOutputFormatClass(CustomerRandomOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, CustomerRandomOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outPath);

        long startTime = System.currentTimeMillis();
        boolean result = job.waitForCompletion(true);
        logger.info("cost time is : {}ms", System.currentTimeMillis() - startTime);

        System.exit(result ? 0 : 1);
    }

}
