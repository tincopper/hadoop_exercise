package com.tomgs.hadoop.test.mapreduce.edts;

import org.apache.hadoop.mapreduce.Partitioner;

import static com.tomgs.hadoop.test.mapreduce.edts.entity.Constants.SEP;

/**
 * 按表进行数据分区
 *
 * @author tangzhongyuan
 * @create 2018-11-14 11:20
 **/
public class EdtsAggregatorPartitioner<K, V> extends Partitioner<K, V> {

    @Override
    public int getPartition(K k, V v, int numPartitions) {
        String key = k.toString();
        String tableId = key.split(SEP)[0];
        return (tableId.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
