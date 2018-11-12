package com.tomgs.hadoop.test.mapreduce.orcfile;

import com.tomgs.hadoop.test.mapreduce.orcfile.customer.CustomerRandomInputFormat;
import com.tomgs.hadoop.test.mapreduce.orcfile.customer.CustomerRandomOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;

/**
 * 用于map计数使用，在伪造数据的时候用到
 * @author tangzhongyuan
 * @create 2018-11-11 18:03
 **/
public class CustomerGenerateOrcData {

    public static class AppendMapper extends Mapper<IntWritable, IntWritable, NullWritable, OrcStruct> {

        private MultipleOutputs<NullWritable, OrcStruct> multipleOutputs;
        private TypeDescription schema = TypeDescription.createStruct();
        private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

        @Override
        protected void map(IntWritable rowId, IntWritable rowNums, Context context)
                throws IOException, InterruptedException {

            InputSplit inputSplit = context.getInputSplit();
            int tableId = (int) inputSplit.getLength();
            int tablePrefix = rowId.get() + tableId * rowNums.get();

            //System.out.println("key:" + key);
            //System.out.println("value" + value);

            //context.write(NullWritable.get(), new Text("key:" + tablePrefix + ",value:" + value));
            //String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            multipleOutputs.write(NullWritable.get(), pair,
                    "table" + tableId + "_" + rowId);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("tableNums", 10);
        conf.setInt("tableRows", 10);

        Job job = Job.getInstance(conf, "CustomerGenerateDataJob");
        job.setJarByClass(CustomerGenerateOrcData.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: CustomerGenerateDataJob <in> [<in>...] <out>");
            System.exit(2);
        }

        job.setMapperClass(AppendMapper.class);

        job.setInputFormatClass(CustomerRandomInputFormat.class);
        //设置下面这个会输出part-m-xxx的
        //job.setOutputFormatClass(CustomerRandomOutputFormat.class);
        //使用下面这个不会有上面的问题
        LazyOutputFormat.setOutputFormatClass(job, OrcOutputFormat.class);
        job.setNumReduceTasks(0);

        FileSystem fileSystem = FileSystem.get(conf);
        Path outpath = new Path(otherArgs[1]);
        if (fileSystem.exists(outpath)) {
            fileSystem.delete(outpath, true);
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, outpath);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
