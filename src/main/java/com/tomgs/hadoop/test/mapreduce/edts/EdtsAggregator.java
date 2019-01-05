package com.tomgs.hadoop.test.mapreduce.edts;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tomgs.hadoop.test.mapreduce.edts.entity.Column;
import com.tomgs.hadoop.test.mapreduce.join.demo7.JoinOrcJob7;
import com.tomgs.hadoop.test.util.DateUtil;
import com.tomgs.hadoop.test.util.HttpUtil;
import com.tomgs.hadoop.test.util.JsonUtil;
import com.tomgs.hadoop.test.util.MapperUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import static com.tomgs.hadoop.test.mapreduce.edts.entity.Constants.*;

/**
 * EDTS数据合并
 *
 * @author tangzhongyuan
 **/
public class EdtsAggregator {

    private static final Logger logger = LoggerFactory.getLogger(EdtsAggregator.class);

    /**
     * 增量数据处理
     */
    public static class EdtsAggregatorAppendDataMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text appendKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String appendData = value.toString();
            if (StringUtils.isBlank(appendData)) {
                return;
            }
            EdtsIncrementalData metaData = JsonUtil.fromJson(appendData, EdtsIncrementalData.class);
            String keyStr = metaData.getTable() + SEP + metaData.getKeyIndexes() + SEP + metaData.getKeyValues();
            appendKey.set(keyStr);
            context.write(appendKey, value);
        }
    }

    /**
     * 全量数据处理
     */
    public static class EdtsAggregatorFullDataMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static Text outputKey = new Text();
        private final static Text outputValue = new Text();

        Map<String, Object> map = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            //获取cacheFile
            FSDataInputStream open = null;
            try {
                URI[] cacheFiles = context.getCacheFiles();
                URI cacheFile = cacheFiles[0];
                Path cachePath = new Path(cacheFile);
                open = FileSystem.get(context.getConfiguration()).open(cachePath);
                String result = open.readUTF();
                map = JsonUtil.convertJsonStrToMap(result);
            } finally {
                if (open != null) {
                    IOUtils.closeStream(open);
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String columnValues = value.toString();
            if (StringUtils.isBlank(columnValues)) {
                return;
            }

            //获取当前map路径
            String filePath = MapperUtil.getFilePath(context);
            System.out.println(filePath);
            if (StringUtils.isBlank(filePath)) {
                return;
            }
            String[] split = filePath.split("/");
            String dbName = split[split.length - 3];
            String tableName = split[split.length - 2];
            String table = dbName + "." + tableName;

            List<Column> keyColumns = getKeyColumns(table, map);
            //说明没有匹配到合适的数据库信息
            if (keyColumns.size() <= 0) {
                return;
            }
            //升序排序
            sortColumns(keyColumns);
            String[] columnValuesArray = columnValues.split(SEP);

            StringBuilder keyIndexs = new StringBuilder();
            StringBuilder keyVlaues = new StringBuilder();
            for (Column keyColumn : keyColumns) {
                keyIndexs.append(keyColumn.getIndex()).append(SEP);
                keyVlaues.append(columnValuesArray[keyColumn.getIndex()]).append(SEP);
            }

            EdtsIncrementalData incData = new EdtsIncrementalData();
            incData.setTable(table);
            incData.setRowValues(value.toString());
            incData.setTimestamp(System.currentTimeMillis());
            incData.setType(ORIGIN);
            incData.setKeyIndexes(keyIndexs.substring(0, keyIndexs.lastIndexOf(SEP)));
            incData.setKeyValues(keyVlaues.substring(0, keyVlaues.lastIndexOf(SEP)));

            String mapKey = table + SEP + incData.getKeyIndexes() + SEP + incData.getKeyValues();
            outputKey.set(mapKey);
            outputValue.set(JsonUtil.toJson(incData));

            context.write(outputKey, outputValue);
        }

        private List<Column> getKeyColumns(String table, Map<String, Object> map) {
            List<Column> columns = new ArrayList<>();
            JSONArray data = (JSONArray) map.get("data"); //JSONArray
            List<JSONObject> jsonObjects = JSONObject.parseArray(data.toJSONString(), JSONObject.class);
            for (JSONObject jsonObject : jsonObjects) {
                String targetTable = jsonObject.getString("targetTable");
                //匹配目标库信息
                if (!StringUtils.equals(table, targetTable)) {
                    continue;
                }
                JSONArray targetColumns = jsonObject.getJSONArray("targetColumns");
                List<JSONObject> targetColumnsObjects =
                        JSONObject.parseArray(targetColumns.toJSONString(), JSONObject.class);
                for (JSONObject targetColumnsObject : targetColumnsObjects) {
                    //获取主键信息
                    Boolean isKey = targetColumnsObject.getBoolean("key");
                    if (isKey) {
                        Column column = targetColumnsObject.toJavaObject(Column.class);
                        columns.add(column);
                    }
                }
            }
            return columns;
        }

        //升序排序
        private void sortColumns(List<Column> columns) {
            Collections.sort(columns, new Comparator<Column>() {
                @Override
                public int compare(Column o1, Column o2) {
                    if (o1.getIndex() >= o2.getIndex()) {
                        return 1;
                    }
                    return -1;
                }
            });
        }
    }

    /**
     * 合并处理<br/>
     * key: tableId_keyValue<br/>
     * value: columeValue<br/>
     */
    public static class EdtsAggregatorMultiReducer extends Reducer<Text, Text, NullWritable, Text> {

        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        @Override
        protected void setup(Context context) {
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

            EdtsDataContainer insertData = new EdtsDataContainer();
            EdtsDataContainer deleteData = new EdtsDataContainer();
            EdtsDataContainer updateData = new EdtsDataContainer();
            EdtsDataContainer originData = new EdtsDataContainer();

            doCacheData(values, insertData, deleteData, updateData, originData);
            //把新增数据插入到原始集合
            originData.addAll(insertData);

            logger.info("inser data nums: {}", insertData.size());
            logger.info("delete data nums: {}", deleteData.size());
            logger.info("update data nums: {}", updateData.size());
            logger.info("origin data nums:{},", originData.size());

            //处理删除和更新
            if (deleteData.size() > 0 || updateData.size() > 0) {
                doUpdateAndDelete(deleteData, updateData, originData);
            }

            String tableInfo = key.toString().split(SEP)[0];
            String[] table = tableInfo.split("\\.");
            String version = DateUtil.getFormat(new Date(), "yyyMMddHH");
            String resultPath = table[0] + "/" + table[1] + "/all@" + version;

            for (EdtsIncrementalData originDatum : originData.listAll()) {
                String rowValues = originDatum.getRowValues();
                multipleOutputs.write(NullWritable.get(), new Text(rowValues), resultPath);
            }
        }
    }

    private static void doCacheData(Iterable<Text> values, EdtsDataContainer insertData, EdtsDataContainer deleteData,
                                    EdtsDataContainer updateData, EdtsDataContainer originData) {
        for (Text value : values) {
            String dataValue = value.toString();
            EdtsIncrementalData incrementalData = JsonUtil.fromJson(dataValue, EdtsIncrementalData.class);
            
            if (INSERT.equals(incrementalData.getType())) {
                insertData.add(incrementalData);
                continue;
            }
            if (UPDATE.equals(incrementalData.getType())) {
                updateData.add(incrementalData);
                continue;
            }
            if (DELETE.equals(incrementalData.getType())) {
                deleteData.add(incrementalData);
                continue;
            }
            if (ORIGIN.equals(incrementalData.getType())) {
                originData.add(incrementalData);
            }
        }
    }

    private static void doUpdateAndDelete(EdtsDataContainer deleteData,
                                          EdtsDataContainer updateData, EdtsDataContainer originData) {
        for (EdtsIncrementalData deleteIncData : deleteData.listAll()) {
            originData.remove(deleteIncData);
        }
        for (EdtsIncrementalData updateIncData : updateData.listAll()) {
            originData.update(updateIncData);
        }
    }

    /**
     * 程序运行main方法
     * @param args 0: 应用名称设置  1: reduce数量设置 2: 元数据地址 3: 增量数据目录 4: 全量文件目录 5: 合并数据临时输出目录 6: 历史数据备份目录<br/>
     *             eg: test10 10 http://10.18.4.58:8080/edts/zone/getAllMaps input/edts/inc input/edts/table input/edts/tmp/output input/tmp/edts/
     * @throws IOException io异常
     * @throws ClassNotFoundException 找不到mapper或者reduce类
     * @throws InterruptedException 中断异常
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 6) {
            throw new IllegalArgumentException("输入参数有误...");
        }

        Configuration conf = new Configuration();
        //设置递归目录
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String appName = otherArgs[0];
        int reduceNums = Integer.parseInt(otherArgs[1]);
        String metadataUrl = otherArgs[2];
        String appendDataDir = otherArgs[3];
        String fullDataDir = otherArgs[4];
        String aggregateTmpOutputDir = otherArgs[5];
        String historyBackupDir = otherArgs[6];

        Job job = Job.getInstance(conf, "EdtsAggregator-" + appName);
        job.setJarByClass(JoinOrcJob7.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(EdtsAggregatorMultiReducer.class);
        job.setPartitionerClass(EdtsAggregatorPartitioner.class);
        job.setNumReduceTasks(reduceNums);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if (!metadataUrl.startsWith("http")) {
            throw new IllegalArgumentException("[元数据地址]输入有误...");
        }
        String httpResult = HttpUtil.get(metadataUrl);
        //test
        //httpResult = "{\"status\":0,\"message\":\"get columnMaps success\",\"data\":[{\"name\":\"debug\",\"channelId\":\"\",\"channelName\":\"Debug\",\"targetId\":\"\",\"targetDatabase\":\"jdbc:mysql://10.40.2.41:3306/test_db\",\"targetTable\":\"test_db.user_table\",\"targetColumns\":[{\"columnName\":\"id\",\"columnValue\":\"\",\"columnType\":0,\"index\":0,\"key\":true},{\"columnName\":\"name\",\"columnValue\":\"\",\"columnType\":0,\"index\":1,\"key\":true},{\"columnName\":\"age\",\"columnValue\":\"\",\"columnType\":0,\"index\":2,\"key\":false}],\"sourceProperties\":{\"key\":\"value\",\"key1\":\"value1\"},\"targetProperties\":{\"key\":\"value\",\"key1\":\"value1\"}}]}";

        Path path = new Path("/tmp/edts/edts_metadata.json");
        FSDataOutputStream os = FileSystem.get(conf).create(path, true);
        os.writeUTF(httpResult);
        IOUtils.closeStream(os);

        job.addCacheFile(path.toUri());

        MultipleInputs.addInputPath(job, new Path(appendDataDir), TextInputFormat.class, EdtsAggregatorAppendDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(fullDataDir), TextInputFormat.class, EdtsAggregatorFullDataMapper.class);

        Path outPath = new Path(aggregateTmpOutputDir);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        job.setOutputFormatClass(EdtsCustomerOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, EdtsCustomerOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outPath);

        long startTime = System.currentTimeMillis();
        boolean result = job.waitForCompletion(true);
        logger.info("aggregate cost time is : {}ms", System.currentTimeMillis() - startTime);
        if (!result) {
            logger.error("code:2, msg:数据聚合失败...");
            System.exit(2);
        }

        //成功，则对数据进行位移
        startTime = System.currentTimeMillis();
        String version = DateUtil.getFormat(new Date(), "yyyMMddHHmmss");
        boolean backupResult = fs.rename(new Path(fullDataDir), new Path(historyBackupDir + version));
        if (!backupResult) {
            logger.error("code:3, msg:历史数据备份失败...");
            System.exit(3);
        }

        boolean aggregateResult = fs.rename(outPath, new Path(fullDataDir));
        if (!aggregateResult) {
            logger.error("code:4, msg:合并数据移动失败...");
            System.exit(4);
        }
        logger.info("backup and move data cost time : {}ms", System.currentTimeMillis() - startTime);

        System.exit(0);
    }
}