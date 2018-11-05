package com.tomgs.hadoop.test.mapreduce;

import com.tomgs.hadoop.test.util.JsonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * @author tangzhongyuan
 * @create 2018-11-05 11:36
 **/
public class ORCDataJoin {

    private static Logger logger = LoggerFactory.getLogger(ORCDataJoin.class);

    private static volatile String appendPath = "";

    public static class OrcFileReadMapper extends Mapper<NullWritable, OrcStruct, Text, NullWritable> {

        private Text outputKey = new Text();


        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();

            Configuration conf = context.getConfiguration();
            //Reader reader = OrcFile.createReader(new Path("my-file.orc"), OrcFile.readerOptions(conf));
            BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path(appendPath))));
            String tmp;
            while ((tmp = reader.readLine()) != null) {

                logger.info("read append data : {}", tmp);

                Map<String, Object> appendMap = JsonUtil.convertJsonStrToMap(tmp);
                int type = (int) appendMap.get("type");
                if (type == 0) { //插入

                }
                if (type == 1) { //更新

                }
                if (type == 2) { //删除

                }
            }
            reader.close();

            sb.append(value.getFieldValue(0));      //通过下标索引获取数据
            sb.append(value.getFieldValue(1));
            sb.append(value.getFieldValue(2));
            sb.append(value.getFieldValue("age"));      //也可以通过字段名获取数据

            // 打印输出获取到的数据
            System.out.println(sb.toString());
            outputKey = new Text(sb.toString());

            context.write(outputKey, NullWritable.get());
        }
    }

    public class OrcFileWriteReducer extends Reducer<Text, IntWritable, NullWritable, OrcStruct> {

        //要创建的ORC文件中的字段类型
        private TypeDescription schema = TypeDescription.fromString(
                "struct<id:string," +
                        "name:string," +
                        "sex:string," +
                        "age:int>"
        );

        private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String line = key.toString();
            String[] lineSplit = line.trim().split(" ");

            pair.setFieldValue(0, new Text(lineSplit[0]));
            pair.setFieldValue(1, new LongWritable(Long.parseLong(lineSplit[1])));
            pair.setFieldValue(2, new Text(lineSplit[2]));
            pair.setFieldValue(3, new Text(lineSplit[3]));
            pair.setFieldValue(4, new Text(lineSplit[4]));

            context.write(NullWritable.get(), pair);
        }
    }

    public static void main(String[] args) {

    }
}
