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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author tomgs
 */
public class RandomGenerateData {

    private static Logger logger = LoggerFactory.getLogger(RandomGenerateData.class);
    private static String opType = "IUD";
    private final FileSystem fileSystem;
    private int rowNums = 10;
    private final String outPath;
    private int index = 0;
    private int[] tableNums;
    private static CountDownLatch cdl;

    public RandomGenerateData(FileSystem fileSystem, int i, int[] tableNums, int rowNums, String outPath) {
        this.fileSystem = fileSystem;
        this.index = i;
        this.tableNums = tableNums;
        this.rowNums = rowNums;
        this.outPath = outPath;
    }

    /**
     * 全量数据
     *
     * @param cols
     * @param records
     */
    void generateData(int cols, int records, int tableId, String outpath) throws IOException {

        //定义输出数据结构
        TypeDescription schema = TypeDescription.createStruct();
        schema.addField("id", TypeDescription.createInt());
        schema.addField("columns", TypeDescription.createInt());
        schema.addField("table", TypeDescription.createInt());
        schema.addField("TS", TypeDescription.createString());
        for (int i = 0; i < cols - 4; ++i) {
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
                String fieldValue = UUID.randomUUID().toString();
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
     *
     * @param cols
     * @param rows
     */
    void generateOp(int cols, int rows, int tableId, FSDataOutputStream fsDataOutputStream) {
        Random random = new Random();
        int tmpRow = 500;//至少500条
        if (rows < 1000) {
            tmpRow += rows;
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
                    json.put("field" + i1, UUID.randomUUID().toString());
                }
            }

            if (opType.charAt(type) == 'U' && i < rows) {
                json.put("id", random.nextInt(rows));
                //随机修改几列
                for (int i1 = 0; i1 < random.nextInt(cols - 4); i1++) {
                    json.put("field" + i1, "field0000" + i1);
                }
            }

            if (opType.charAt(type) == 'D' && i < rows) {
                json.put("id", random.nextInt(rows));
            }

            if (json.size() <= 0) {
                continue;
            }

            long currentTimeMillis = System.currentTimeMillis();
            JSONObject item = new JSONObject();
            try {
                item.put("TS", currentTimeMillis);
                item.put("type", type);
                item.put("table", tableId);
                item.put("data", json);
                fsDataOutputStream.writeBytes(item.toString());
                fsDataOutputStream.writeBytes("\n");
                fsDataOutputStream.flush();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    class AllGenerateData implements Runnable {

        @Override
        public void run() {
            //生成全量
            try {
                generateData(tableNums[index], rowNums, index, outPath + "/table/table" + index + ".orc");
                logger.info("写table{}.orc成功，剩余{}个文件待写...", index, cdl.getCount() - 1);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            } finally {
                cdl.countDown();
            }
        }
    }

    class AppendGenerateData implements Runnable {

        @Override
        public void run() {
            try {
                //生成增量
                FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(outPath + "/inc/inc" + index + ".json"));
                generateOp(tableNums[index], rowNums, index, fsDataOutputStream);
                logger.info("写table{}/inc{}.json" + "成功，剩余{}个文件待写...", index, index, cdl.getCount() - 1);

                if (fsDataOutputStream != null) {
                    fsDataOutputStream.flush();
                    fsDataOutputStream.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            } finally {
                cdl.countDown();
            }
        }
    }

    /**
     * 运行 java -classpath *.jar com.tomgs.hadoop.test.mapreduce.join.RandomGenerateData 10 10 input/table 10
     * 最后一个代表线程数据量
     *
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
        for (int i = 0; i < tableNum; ++i) {
            tableNums[i] = random.nextInt(50) + 15;
        }

        //设置输出目录
        final String outPath = args[2];
        System.out.println("文件输出目录:" + outPath);

        final String threads = args[3];
        System.out.println("线程数:" + threads);

        final String startTableId = args[6];
        System.out.println("初始表ID:" + startTableId);

        String HDFS_ROOT_PATH = args[4];
        String hdfsUser = args[5];
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
        int startTable = Integer.parseInt(startTableId);
        cdl = new CountDownLatch((tableNums.length - startTable) * 2);

        for (int i = 0; i < tableNums.length - startTable; i++) {
            int currentIndex = i + startTable;
            RandomGenerateData randomGenerateData = new RandomGenerateData(fileSystem, currentIndex, tableNums, tableRowNums, outPath);
            executorService.submit(randomGenerateData.new AllGenerateData());
            executorService.submit(randomGenerateData.new AppendGenerateData());
        }

        long startTime = System.currentTimeMillis();
        cdl.await();
        logger.info("write data cost time : {}ms", System.currentTimeMillis() - startTime);

        executorService.shutdown();
    }

}
