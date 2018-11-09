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
import java.net.URI;
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
    private final FileSystem fileSystem;
    private int rowNums = 10;
    private final String outPath;
    private int index = 0;
    private int[] tableNums;

    public RandomGenerateData(FileSystem fileSystem, int i, int[] tableNums, int rowNums, String outPath) {
        this.fileSystem = fileSystem;
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
     void generateData(int cols, int records, int tableId, String outpath) throws IOException {
        //Configuration conf = new Configuration();

        //定义输出数据结构
        TypeDescription schema = TypeDescription.createStruct();
        schema.addField("id", TypeDescription.createInt());
        schema.addField("columns", TypeDescription.createInt());
        schema.addField("table", TypeDescription.createInt());
        schema.addField("TS", TypeDescription.createString());
        for (int i = 0; i < cols - 4; ++ i) {
            schema.addField("field" + i, TypeDescription.createString());
        }

        Writer writer = OrcFile.createWriter(new Path(outpath), OrcFile.writerOptions(fileSystem.getConf())
                .fileSystem(fileSystem).setSchema(schema));
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
    void generateOp(int cols, int rows, int tableId, FSDataOutputStream fsDataOutputStream) {
        Random random = new Random();
        int tmpRow = 50;//至少50条
        if (rows < 10050) {
            tmpRow += rows / 2;
        } else {
            tmpRow += random.nextInt(10000);
        }
        for (int i = 0; i < tmpRow; i++) { //操作行数的一半的数量

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
                    json.put("field" + i1, "field0000" + i1);
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
                item.put("data", json);
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

        try {
            //生成全量
            generateData(this.tableNums[index], rowNums, index, outPath + "/table/table" + index + ".orc");
            System.out.println("写table" + index + ".orc成功...");
            //生成增量
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
            tableNums[i] = random.nextInt(100) + 15;
        }

        //设置输出目录
        final String outPath = args[2];
        System.out.println("文件输出目录:" + outPath);

        final String threads = args[3];
        System.out.println("线程数:" + threads);

        //FileSystem fileSystem = FileSystem.get(new URI(args[3]), configuration);
        //FileSystem fileSystem = FileSystem.get(configuration);

        String HDFS_ROOT_PATH = args[4];
        String hdfsUser = args[5];
        //configuration.set("fs.defaultFS", "hdfs://dwhdponline");
        FileSystem fileSystem = null;
        Configuration configuration = new Configuration();
        if ("localhost".equalsIgnoreCase(HDFS_ROOT_PATH)) {
            fileSystem = FileSystem.get(configuration);
        } else {
            configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            fileSystem = FileSystem.get(new URI(HDFS_ROOT_PATH), configuration, hdfsUser);
        }

        Configuration conf = fileSystem.getConf();
        System.out.println(conf == configuration);

        ExecutorService executorService = Executors.newFixedThreadPool(Integer.parseInt(threads));
        for (int i = 0; i < tableNums.length; i++) {
            executorService.submit(new RandomGenerateData(fileSystem, i, tableNums, tableRowNums, outPath));
        }
        executorService.shutdown();
    }

}
