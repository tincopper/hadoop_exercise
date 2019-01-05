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

import java.io.IOException;

/**
 * 用于map计数使用，在伪造数据的时候用到
 * @author tangzhongyuan
 * @create 2018-11-11 18:03
 **/
public class CustomerGenerateData {

    public static class AppendMapper extends Mapper<IntWritable, IntWritable, NullWritable, Text> {

        private MultipleOutputs<NullWritable, Text> multipleOutputs;

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
            int startId = rowId.get();
            int length = rowNums.get();
            StringBuffer sb = new StringBuffer();
            for (int i = startId; i < length; i++) {
                //tableId,cloumsNum,timestamp,rowId,field1Value,field2Value
                sb.append("key:").append(i).append(",value").append(rowNums).append("\n");
            }
            multipleOutputs.write(NullWritable.get(), new Text(sb.toString()),
                    "table" + tableId + "_" + rowId);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("tableNums", 20);
        conf.setInt("tableRows", 20);
        conf.setInt("startIndex", 0);
        conf.setInt("splitsRows", 1); //表的分片数

        Job job = Job.getInstance(conf, "CustomerGenerateDataJob");
        job.setJarByClass(CustomerGenerateData.class);

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
        LazyOutputFormat.setOutputFormatClass(job, CustomerRandomOutputFormat.class);
        job.setNumReduceTasks(0);

        //job.setInputFormatClass(CustomerRandomInputFormat.class);
        //job.setOutputKeyClass(NullWritable.class);
        //job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(0);

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
