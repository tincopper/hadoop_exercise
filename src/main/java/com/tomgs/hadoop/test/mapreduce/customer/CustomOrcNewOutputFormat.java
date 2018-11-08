package com.tomgs.hadoop.test.mapreduce.customer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

/**
 * 实现输出文件名和输入文件名一致
 * @author tangzhongyuan
 * @create 2018-09-26 9:57
 **/
public class CustomOrcNewOutputFormat extends OrcNewOutputFormat {

    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);

        StringBuilder result = new StringBuilder();
        result.append(getUniqueFile(context, getOutputName(context), extension)).append(".orc");

        return new Path(committer.getWorkPath(), result.toString());
    }
}
