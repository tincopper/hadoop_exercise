package com.tomgs.hadoop.test.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;

/**
 * @author tangzhongyuan
 * @create 2018-11-06 11:55
 **/
public class ReaderExample {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        String orcPath = "E:\\workspace\\idea\\hadoop_exercise\\output\\joinorcjob1\\part-r-00000.orc";
        orcPath = "E:\\workspace\\idea\\hadoop_exercise\\output\\joinorcjob2\\table_10.orc";
        orcPath = "E:\\workspace\\idea\\hadoop_exercise\\output\\join_demo_10\\part-m-00000.orc";
        orcPath = "E:\\workspace\\idea\\hadoop_exercise\\input\\join\\table\\table1.orc";// 错误
        orcPath = "E:\\workspace\\idea\\hadoop_exercise\\input\\join\\table2\\demo9.orc";
        orcPath = "E:\\workspace\\idea\\hadoop_exercise\\output\\join_demo_11\\part-m-00000.orc";
        orcPath = "E:\\workspace\\idea\\hadoop_exercise\\input\\join\\table\\table0.orc";
        orcPath = "E:\\workspace\\idea\\hadoop_exercise\\input\\join\\table1.orc";

        // 使用 OrcFile 创建 Reader
        Reader reader = OrcFile.createReader(new Path(orcPath), OrcFile.readerOptions(conf));

        // 读取文件
        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();

        while (rows.nextBatch(batch)) {
            System.out.println(batch.size);
            System.out.println(batch);
        }
        rows.close();
    }
}