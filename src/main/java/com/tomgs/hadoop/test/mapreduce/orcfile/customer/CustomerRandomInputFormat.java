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
        int startIndex = configuration.getInt("startIndex", 0);

        List<InputSplit> inputSplits = new ArrayList<>();

        Path outDir = FileOutputFormat.getOutputPath(job);
        for (int i = 0; i < tableNums; i++) {
            inputSplits.add(new FileSplit(new Path(outDir, "split-" + i),
                    startIndex, startIndex + i, null));
        }

        return inputSplits;
    }
}

class RandomRecordReader extends RecordReader<IntWritable, IntWritable> {

    private int tableNums;
    private int rowNums;
    private int splitsRows;

    private IntWritable key;
    private IntWritable value;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        tableNums = configuration.getInt("tableNums", 0) - 1;
        rowNums = configuration.getInt("tableRows", 0);
        splitsRows = configuration.getInt("splitsRows", 0);
    }

    int index = 0;
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (tableNums <= 0 || rowNums <= 0) {
            return false;
        }
        int limit = rowNums / splitsRows;
        if (key == null) {
            key = new IntWritable();
            key.set(0);
        }
        if (value == null) {
            value = new IntWritable();
            value.set(limit);
            return true;
        }

        int currentValue = value.get();
        if (currentValue >= rowNums) {
            return false;
        }
        if (rowNums % splitsRows != 0) {
            return false;
        }
        index += limit;
        key.set(index);
        value.set(index + limit);
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
            return Math.min(1.0f, (value.get()) / (float) (rowNums));
        }
    }

    @Override
    public void close() {

    }
}