package com.tomgs.hadoop.test.mapreduce.orcfile.customer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author tangzhongyuan
 * @create 2018-11-11 18:09
 **/
public class CustomerRandomInputFormat extends FileInputFormat<IntWritable, IntWritable> {

    @Override
    public RecordReader<IntWritable, IntWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {

        return new RandomRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        Configuration configuration = context.getConfiguration();
        int tableNums = configuration.getInt("tableNums", -1);
        if (tableNums <= 0) {
            return false;
        }
        return true;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        Configuration configuration = job.getConfiguration();
        int tableNums = configuration.getInt("tableNums", 0);
        List<InputSplit> inputSplits = new ArrayList<>();

        Path outDir = FileOutputFormat.getOutputPath(job);
        for (int i = 0; i < tableNums; i++) {
            inputSplits.add(new FileSplit(new Path(outDir, "split-" + i),
                    0, i, null));
        }

        return inputSplits;
    }
}

class RandomRecordReader extends RecordReader<IntWritable, IntWritable> {

    private int tableNums;
    private int rowNums;

    private IntWritable key;
    private IntWritable value;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        tableNums = configuration.getInt("tableNums", 0) - 1;
        rowNums = configuration.getInt("tableRows", 0);

    }

    private int index = 0;

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (tableNums <= 0 || rowNums <= 0) {
            return false;
        }
        if (key == null) {
            key = new IntWritable();
            key.set(0);
        }
        if (value == null) {
            value = new IntWritable();
            value.set(rowNums);
        }

        int currentKey = key.get();
        if (currentKey >= rowNums) {
            return false;
        }
        key.set(index++);
        return true;
    }

    @Override
    public IntWritable getCurrentKey() {
        return key;
    }

    @Override
    public IntWritable getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        if (key.get() == 0) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (key.get()) / (float) (tableNums));
        }
    }

    @Override
    public void close() {

    }
}