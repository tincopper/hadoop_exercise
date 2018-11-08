package com.tomgs.hadoop.test.mapreduce.customer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author tangzhongyuan
 * @create 2018-11-08 11:16
 **/
public class OrcCustomerOutputFormat<V extends Writable>
        extends FileOutputFormat<NullWritable, V> {

    private static final String EXTENSION = ".orc";

    // output committer.
    public static final String SKIP_TEMP_DIRECTORY =
            "orc.mapreduce.output.skip-temporary-directory";

    @Override
    public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        Path filename = getDefaultWorkFile(job, EXTENSION);
        FileSystem fileSystem = FileSystem.get(conf);

        //Writer writer = OrcFile.createWriter(filename, org.apache.orc.mapred.OrcOutputFormat.buildOptions(conf));
        return new OrcCustomMapreduceRecordWriter(filename, fileSystem);
    }

    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context,
                                   String extension) throws IOException {
        if (context.getConfiguration().getBoolean(SKIP_TEMP_DIRECTORY, false)) {
            return new Path(getOutputPath(context),
                    getUniqueFile(context, getOutputName(context), extension));
        } else {
            return super.getDefaultWorkFile(context, extension);
        }
    }
}
