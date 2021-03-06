package com.tomgs.hadoop.test.mapreduce.join.demo4;

import com.tomgs.hadoop.test.mapreduce.join.demo2.MainJob;
import com.tomgs.hadoop.test.util.JsonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * @author tangzhongyuan
 * @create 2018-11-06 9:37
 **/
public class JoinOrcJob2 {
    private static Logger logger = LoggerFactory.getLogger(JoinOrcJob2.class);
    public static class MultiReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

        //private TypeDescription schema = TypeDescription.fromString(structInfo);
        //private final TypeDescription schema = TypeDescription.createStruct();
        //private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

        private String orcOutputPath;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            orcOutputPath = context.getConfiguration().get("orc_output_path", "E://output.orc");
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            if (values == null) {
                return;
            }
            long startTime = System.currentTimeMillis();

            List<String> insertData = new ArrayList<>();
            List<String> deleteData = new ArrayList<>();
            List<String> updateData = new ArrayList<>();
            List<String> originData = new ArrayList<>();

            doCacheData(values, insertData, deleteData, updateData, originData);
            //把新增数据插入到原始集合
            originData.addAll(insertData);

            //要写入的内容
            Map<String, Object> firstMap = JsonUtil.convertJsonStrToMap(originData.get(0));

            if (insertData.size() <= 0 && deleteData.size() <= 0 && updateData.size() <= 0) {
                context.write(NullWritable.get(), new Text("table:" + firstMap.get("table") + " no data can be convert..."));
                return;
            }

            //处理删除和更新
            if (deleteData.size() > 0 || updateData.size() > 0) {
                doUpdateAndDelete(deleteData, updateData, originData);
            }

            Configuration conf = new Configuration();
            FileSystem.get(conf);

            int filedColumns = (int) firstMap.get("columns");

            TypeDescription schema = TypeDescription.createStruct();
            schema.addField("id", TypeDescription.createInt());
            schema.addField("columns", TypeDescription.createInt());
            schema.addField("table", TypeDescription.createInt());
            schema.addField("TS", TypeDescription.createString());
            for (int i = 0; i < filedColumns - 4; i++) {
                schema.addField("field" + i, TypeDescription.createString());
            }
            String resultPath = orcOutputPath + "/" + key.toString() + "/table_" + key.toString() + ".orc";

            Path outPath = new Path(resultPath);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }

            Writer writer = OrcFile.createWriter(new Path(resultPath),
                    OrcFile.writerOptions(conf).setSchema(schema));
            VectorizedRowBatch batch = schema.createRowBatch();

            for (String originDatum : originData) {
                Map<String, Object> map = JsonUtil.convertJsonStrToMap(originDatum);

                int id = Integer.parseInt((String) map.get("id"));
                int columns = (int) map.get("columns");
                int table = (int) map.get("table");
                String timestamp = (String) map.get("TS");
                //int type = (int) map.get("type");

                int row = batch.size++;
                ((LongColumnVector) batch.cols[0]).vector[row] = id;
                ((LongColumnVector) batch.cols[1]).vector[row] = columns;
                ((LongColumnVector) batch.cols[2]).vector[row] = table;
                ((BytesColumnVector) batch.cols[3]).setVal(row, timestamp.getBytes());
                for (int i = 0; i < map.size() - 4; i++) {
                    String fieldValue = (String) map.get("field" + i);
                    if (fieldValue == null) {
                        logger.info("----> get value form map is null, key is [{}]", "field" + i);
                        break;
                    }
                    ((BytesColumnVector) batch.cols[i + 4]).setVal(row, fieldValue.getBytes());
                    //batch full
                    if (batch.size == batch.getMaxSize()) {
                        writer.addRowBatch(batch);
                        batch.reset();
                    }
                }

            }
            if (batch.size != 0) {
                //logger.info("------>\n{}", batch.toString());
                logger.info("---> wirte batch size is [{}]", batch.size);
                writer.addRowBatch(batch);
                batch.reset();
            }
            if (writer != null) {
                writer.close();
            }

            context.write(NullWritable.get(), new Text("convert table:" + firstMap.get("table")
                    +", cost time : " + (System.currentTimeMillis() - startTime) + "ms."));
        }

        public static void doCacheData(Iterable<Text> values, List<String> insertData,
                                       List<String> deleteData, List<String> updateData, List<String> originData) {
            for (Text value : values) {
                String dataValue = value.toString();
                if (dataValue.startsWith("I")) {
                    insertData.add(dataValue.substring(1));
                    continue;
                }
                if (dataValue.startsWith("U")) {
                    updateData.add(dataValue.substring(1));
                    continue;
                }
                if (dataValue.startsWith("D")) {
                    deleteData.add(dataValue.substring(1));
                    continue;
                }
                if (dataValue.startsWith("{")) {
                    originData.add(dataValue);
                    continue;
                }
            }
        }

        public static void doUpdateAndDelete(List<String> deleteData, List<String> updateData, List<String> originData) {
            for (ListIterator<String> originIterator = originData.listIterator(); originIterator.hasNext(); ) {
                String originDatum = originIterator.next();
                Map<String, Object> originDatumMap = JsonUtil.convertJsonStrToMap(originDatum);
                String id = String.valueOf(originDatumMap.get("id"));

                //删除处理
                for (ListIterator<String> deleteIterator = deleteData.listIterator(); deleteIterator.hasNext(); ) {
                    String deleteDatum = deleteIterator.next();
                    Map<String, Object> deleteDatumMap = JsonUtil.convertJsonStrToMap(deleteDatum);
                    if (!deleteDatumMap.containsKey("id")) {
                        continue;
                    }
                    String deleteId = String.valueOf(deleteDatumMap.get("id"));
                    if (deleteId.equals(id)) {
                        originIterator.remove();
                        deleteIterator.remove();
                        originDatumMap = null;
                        break;
                    }
                }

                //更新处理
                for (ListIterator<String> updateIterator = updateData.listIterator(); updateIterator.hasNext(); ) {
                    String updateDatum = updateIterator.next();
                    Map<String, Object> updateDatumMap = JsonUtil.convertJsonStrToMap(updateDatum);
                    if (!updateDatumMap.containsKey("id")) {
                        continue;
                    }
                    String updateId = String.valueOf(updateDatumMap.get("id"));
                    if (updateId.equals(id)) {
                        //说明已经删除
                        if (originDatumMap == null) {
                            break;
                        }
                        //删除多余字段
                        updateDatumMap.remove("columns");
                        updateDatumMap.remove("table");
                        updateDatumMap.remove("type");

                        originDatumMap.putAll(updateDatumMap);

                        String updateJson = JsonUtil.toJson(originDatumMap);

                        originIterator.remove();
                        originIterator.add(updateJson);
                        break;
                    }
                }

            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 4) {
            throw new IllegalArgumentException("输入参数有误...");
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.set("orc_output_path", otherArgs[3]);

        Job job = Job.getInstance(conf, "demo4-join");
        job.setJarByClass(JoinOrcJob2.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MultiReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MainJob.AppendMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), OrcInputFormat.class, MainJob.OrcFileReadMapper.class);

        Path outPath = new Path(otherArgs[2]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        //job.setOutputFormatClass(OrcOutputFormat.class);不能丢，不然不能输出orc文件
        //job.setOutputFormatClass(OrcOutputFormat.class);
        //OrcOutputFormat.setOutputPath(job, outPath);
        FileOutputFormat.setOutputPath(job, outPath);
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
