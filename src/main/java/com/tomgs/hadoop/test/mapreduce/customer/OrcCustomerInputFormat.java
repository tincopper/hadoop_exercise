package com.tomgs.hadoop.test.mapreduce.customer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.mapred.OrcInputFormat;
import org.apache.orc.mapreduce.OrcMapreduceRecordReader;

import java.io.IOException;

/**
 * @author tangzhongyuan
 * @create 2018-11-10 1:37
 **/
public class OrcCustomerInputFormat<V extends WritableComparable>
        extends FileInputFormat<NullWritable, V> {

    /**
     * Put the given SearchArgument into the configuration for an OrcInputFormat.
     * @param conf the configuration to modify
     * @param sarg the SearchArgument to put in the configuration
     * @param columnNames the list of column names for the SearchArgument
     */
    public static void setSearchArgument(Configuration conf,
                                         SearchArgument sarg,
                                         String[] columnNames) {
        org.apache.orc.mapred.OrcInputFormat.setSearchArgument(conf, sarg,
                columnNames);
    }

    @Override
    public RecordReader<NullWritable, V>
    createRecordReader(InputSplit inputSplit,
                       TaskAttemptContext taskAttemptContext
    ) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();
        Reader file = OrcFile.createReader(split.getPath(),
                OrcFile.readerOptions(conf)
                        .maxLength(Long.MAX_VALUE));
        return new OrcMapreduceRecordReader<>(file,
                OrcInputFormat.buildOptions(conf, file, split.getStart(), split.getLength()));
    }
}
