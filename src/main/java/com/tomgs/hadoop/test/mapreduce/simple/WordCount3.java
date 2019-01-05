package com.tomgs.hadoop.test.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * @author tangzhongyuan
 * @create 2018-12-10 16:41
 **/
public class WordCount3 {

    public static class TokenizerMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {

        static enum Counters {
            INPUT_WORDS
        };

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private long numRecords = 0;

        @Override
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
                reporter.incrCounter(Counters.INPUT_WORDS, 1);
            }

            if ((++numRecords % 100) == 0) {
                reporter.setStatus("Finished processing " + numRecords + " records "
                        + "from the input file");
            }
        }
    }

    public static class IntSumReducer extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }
}
