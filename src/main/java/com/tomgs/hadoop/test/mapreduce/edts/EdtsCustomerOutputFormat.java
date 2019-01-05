package com.tomgs.hadoop.test.mapreduce.edts;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author tangzhongyuan
 * @create 2018-11-12 10:51
 **/
public class EdtsCustomerOutputFormat<K, V> extends TextOutputFormat<K, V> {

    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
        StringBuilder result = new StringBuilder();
        String outputName = getOutputName(context);
        if (outputName == null) {
            return committer.getWorkPath();
        }
        result.append(outputName).append(".txt");

        return new Path(committer.getWorkPath(), result.toString());
    }

    protected static String getOutputName(JobContext job) {
        String outputName = job.getConfiguration().get(BASE_OUTPUT_NAME);
        return outputName;
    }

}
