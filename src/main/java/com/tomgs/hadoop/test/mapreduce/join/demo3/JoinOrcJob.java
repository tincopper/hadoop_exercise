package com.tomgs.hadoop.test.mapreduce.join.demo3;

import com.tomgs.hadoop.test.mapreduce.join.demo2.MainJob;
import com.tomgs.hadoop.test.mapreduce.join.demo4.JoinOrcJob2;
import com.tomgs.hadoop.test.util.JsonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;
import java.util.ListIterator;
import java.util.Map;
import java.util.Vector;

/**
 * 
 * @author tomgs
 *
 */
public class JoinOrcJob {

    private static String structInfo = "struct<columns:int,table:int,type:int,id:string,timestamp:string,field1:string,field2:string,field3:string>";

    public static class MultiReducer extends Reducer<IntWritable, Text, NullWritable, OrcStruct> {

        private TypeDescription schema = TypeDescription.fromString(structInfo);
        private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);
        private final NullWritable nada = NullWritable.get();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            //OrcConf.MAPRED_OUTPUT_SCHEMA.setString(context.getConfiguration(), structInfo);
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

            if (deleteData.size() > 0 || updateData.size() > 0) {
                //处理删除和更新
                JoinOrcJob2.MultiReducer.doUpdateAndDelete(deleteData, updateData, originData);
            }
            /*StringBuffer sb = new StringBuffer(1024 * 100);
            for (String originDatum : originData) {
                sb.append(originDatum).append("\n");
            }*/
            //columns:int,table:int,type:int,id:string,timestamp:string,field1:string,field2:string,field3:string
            for (String originDatum : originData) {
                Map<String, Object> map = JsonUtil.convertJsonStrToMap(originDatum);
                int columns = (int) map.get("columns");
                int table = (int) map.get("table");
                int type = (int) map.get("type");
                String id = (String) map.get("id");
                String timestamp = (String) map.get("timestamp");

                IntWritable columnsWritable = new IntWritable();
                columnsWritable.set(columns);
                pair.setFieldValue(0, columnsWritable);

                IntWritable tableWritable = new IntWritable();
                tableWritable.set(table);
                pair.setFieldValue(1, tableWritable);

                IntWritable typeWritable = new IntWritable();
                typeWritable.set(type);
                pair.setFieldValue(2, typeWritable);

                Text idWritable = new Text();
                idWritable.set(id);
                pair.setFieldValue(3, idWritable);

                Text timestampWritable = new Text();
                timestampWritable.set(timestamp);
                pair.setFieldValue(4, timestampWritable);

                for (int i = 0; i < map.size() - 5; i++) {
                    Text fieldWritable = new Text();
                    String filedValue = (String) map.get("field" + (i + 1));
                    fieldWritable.set(filedValue == null ? "" : filedValue);
                    pair.setFieldValue(i + 5, fieldWritable);
                }

                context.write(nada, pair);
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, structInfo);

        Job job = Job.getInstance(conf, "demo3-join");
        job.setJarByClass(JoinOrcJob.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MultiReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrcStruct.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MainJob.AppendMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), OrcInputFormat.class, MainJob.OrcFileReadMapper.class);

        Path outPath = new Path(args[2]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        //job.setOutputFormatClass(OrcOutputFormat.class);不能丢，不然不能输出orc文件
        job.setOutputFormatClass(OrcOutputFormat.class);
        OrcOutputFormat.setOutputPath(job, outPath);
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
