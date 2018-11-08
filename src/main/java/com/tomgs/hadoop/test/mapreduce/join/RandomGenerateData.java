package com.tomgs.hadoop.test.mapreduce.join;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 
 * @author tomgs
 *
 */
public class RandomGenerateData extends Thread {

    private static Logger logger = LoggerFactory.getLogger(RandomGenerateData.class);
    private static String opType = "IUD";
    private int rowNums = 10;
    private final String outPath;
    private int index = 0;
    private int[] tableNums;

    public RandomGenerateData(int i, int[] tableNums, int rowNums, String outPath) {
        this.index = i;
        this.tableNums = tableNums;
        this.rowNums = rowNums;
        this.outPath = outPath;
    }

    /**
     * 全量数据
     * @param cols
     * @param records
     */
    static void generateData(int cols, int records, int tableId, String outpath) throws IOException {
        Configuration conf = new Configuration();

        //定义输出数据结构
        TypeDescription schema = TypeDescription.createStruct();
        schema.addField("id", TypeDescription.createInt());
        schema.addField("columns", TypeDescription.createInt());
        schema.addField("table", TypeDescription.createInt());
        schema.addField("TS", TypeDescription.createString());
        for (int i = 0; i < cols - 4; ++ i) {
            schema.addField("field" + i, TypeDescription.createString());
        }

        Writer writer;
        try {
            Path outPath = new Path(outpath);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }

            writer = OrcFile.createWriter(outPath, OrcFile.writerOptions(conf).setSchema(schema));
        } catch(Exception e) {
            System.out.println(e);
            return;
        }

        VectorizedRowBatch batch = schema.createRowBatch();

        //生成数据
        for (int i = 0; i < records; i++) {
            int id = i;
            int columns = cols;
            int table = tableId;
            String timestamp = String.valueOf(System.currentTimeMillis());

            int row = batch.size++;
            ((LongColumnVector) batch.cols[0]).vector[row] = id;
            ((LongColumnVector) batch.cols[1]).vector[row] = columns;
            ((LongColumnVector) batch.cols[2]).vector[row] = table;
            ((BytesColumnVector) batch.cols[3]).setVal(row, timestamp.getBytes());

            for (int j = 0; j < columns - 4; j++) {
                String fieldValue = "field" + j;
                ((BytesColumnVector) batch.cols[j + 4]).setVal(row, fieldValue.getBytes());
            }

            //batch full
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }

        if (batch.size != 0) {
            logger.info("---> wirte batch size is [{}]", batch.size);
            writer.addRowBatch(batch);
            batch.reset();
        }
        if (writer != null) {
            writer.close();
        }
    }

    /**
     * 更改数据
     * @param cols
     * @param rows
     */
    static void generateOp(int cols, int rows, int tableId, FSDataOutputStream fsDataOutputStream) {
        Random random = new Random();
        for (int i = 0; i < cols / 2; i++) { //操作列数的一半的数量

            JSONObject json = new JSONObject();
            int type = random.nextInt(opType.length());

            if (opType.charAt(type) == 'I') {
                long currentTimeMillis = System.currentTimeMillis();
                json.put("id", ++rows);
                json.put("table", tableId);
                json.put("columns", cols);
                json.put("TS", currentTimeMillis);
                for (int i1 = 0; i1 < cols - 4; i1++) {
                    json.put("field" + i1, "field" + i1);
                }
            }

            if (opType.charAt(type) == 'U') {
                json.put("id", random.nextInt(rows));
                //随机修改几列
                for (int i1 = 0; i1 < random.nextInt(cols - 4); i1++) {
                    json.put("field" + i1, "field00" + i1);
                }
            }

            if (opType.charAt(type) == 'D') {
                json.put("id", random.nextInt(rows));
            }

            long currentTimeMillis = System.currentTimeMillis();
            //json.put("TS", currentTimeMillis);
            //json.put("type", type);
            //json.put("table", tableId);
            //json.put("columns", cols);
            JSONObject item = new JSONObject();
            try {
                item.put("TS", currentTimeMillis);
                item.put("type", type);
                item.put("table", tableId);
                item.put(String.format("%c", opType.charAt(type)), json);
                fsDataOutputStream.writeBytes(item.toString());
                fsDataOutputStream.writeBytes("\n");
            } catch(Exception e) {

            }

            try {
                fsDataOutputStream.flush();
            } catch(Exception e) {

            }
        }
    }

    @Override
    public void run() {

        //生成全量
        try {
            generateData(this.tableNums[index], rowNums, index, outPath + "/table/table" + index + ".orc");
            System.out.println("写table" + index + "成功...");
            //生成增量
            FileSystem fileSystem = FileSystem.get(new Configuration());
            FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(outPath + "/inc/inc" + index + ".json"));
            generateOp(tableNums[this.index], rowNums, this.index, fsDataOutputStream);
            System.out.println("写table" + index + " /inc" + index + ".json" + "成功...");

            if (fsDataOutputStream != null) {
                fsDataOutputStream.flush();
                fsDataOutputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 运行 java -classpath *.jar com.tomgs.hadoop.test.mapreduce.join.RandomGenerateData 10 10 input/table 10
     * 最后一个代表线程数据量
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        //设置表的数量
        int tableNum = Integer.parseInt(args[0]);
        System.out.println("生成表的数量:" + tableNum);

        //设置表内容行数
        final int tableRowNums = Integer.parseInt(args[1]);
        System.out.println("生成表的行数:" + tableRowNums);

        //表的列数
        final int[] tableNums = new int[tableNum];
        //设置每个表的列数
        Random random = new Random();
        for (int i = 0; i < tableNum - 1; ++ i) {
            //生成5-35列的数据,保证除了4个必须字段外至少还有一个field字段
            tableNums[i] = random.nextInt(30) + 5;
        }

        //设置输出目录
        final String outPath = args[2];

        ExecutorService executorService = Executors.newFixedThreadPool(Integer.parseInt(args[3]));
        for (int i = 0; i < tableNums.length; i++) {
            executorService.submit(new RandomGenerateData(i, tableNums, tableRowNums, outPath));
        }
        executorService.shutdown();
    }

}
