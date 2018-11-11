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

        String orcPath = "E:\\workspace\\idea\\hadoop_exercise\\input\\orc\\table381.orc";

        // 使用 OrcFile 创建 Reader
        Reader reader = OrcFile.createReader(new Path(orcPath), OrcFile.readerOptions(conf));

        // 读取文件
        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        int i = 0;
        while (rows.nextBatch(batch)) {
            i+=batch.size;
            //System.out.println(batch.size);
            //System.out.println(batch);
        }
        System.out.println("all size:" + i);
        rows.close();
    }
}