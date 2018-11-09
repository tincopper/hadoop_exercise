package com.tomgs.hadoop.test.mapreduce.customer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcMapredRecordWriter;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

import java.io.IOException;

/**
 * 实现 RecordWriter 接口，泛型为mapreduce要写的key-value键值对
 * @author tangzhongyuan
 * @create 2018-11-08 11:20
 **/
public class OrcCustomMapreduceRecordWriter<V extends Writable>
        extends RecordWriter<NullWritable, V> {

    private Path fiilename;
    private FileSystem fileSystem;
    private Writer writer;
    private VectorizedRowBatch batch;
    private TypeDescription schema;
    private boolean isTopStruct;

    public OrcCustomMapreduceRecordWriter(Path filename, FileSystem fileSystem) {
        this.fiilename = filename;
        this.fileSystem = fileSystem;
    }

    /**
     * 这个就是真正的去写文件内容了
     * @param key null
     * @param value OrcStruct
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(NullWritable key, V value) throws IOException {
        if (writer == null) {
            writer = OrcFile.createWriter(fiilename,
                    OrcFile.writerOptions(fileSystem.getConf()).setSchema(((OrcStruct)value).getSchema()));
            schema = writer.getSchema();
            this.batch = schema.createRowBatch();
            isTopStruct = schema.getCategory() == TypeDescription.Category.STRUCT;
        }

        // if the batch is full, write it out.
        if (batch.size == batch.getMaxSize()) {
            writer.addRowBatch(batch);
            batch.reset();
        }

        // add the new row
        int row = batch.size++;
        // skip over the OrcKey or OrcValue
        if (value instanceof OrcKey) {
            value = (V)((OrcKey) value).key;
        } else if (value instanceof OrcValue) {
            value = (V)((OrcValue) value).value;
        }
        if (isTopStruct) {
            for(int f=0; f < schema.getChildren().size(); ++f) {
                OrcMapredRecordWriter.setColumn(schema.getChildren().get(f),
                        batch.cols[f], row, ((OrcStruct) value).getFieldValue(f));
            }
        } else {
            OrcMapredRecordWriter.setColumn(schema, batch.cols[0], row, value);
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        if (batch != null && batch.size != 0) {
            writer.addRowBatch(batch);
        }
        if (writer != null) {
            writer.close();
        }
    }
}
