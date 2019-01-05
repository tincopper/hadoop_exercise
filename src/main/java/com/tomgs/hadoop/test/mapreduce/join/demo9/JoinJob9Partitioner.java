package com.tomgs.hadoop.test.mapreduce.join.demo9;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author tangzhongyuan
 * @create 2018-11-14 11:20
 **/
public class JoinJob9Partitioner<K, V> extends Partitioner<K, V> {

    @Override
    public int getPartition(K k, V v, int numPartitions) {
        String key = k.toString();
        String tableId = key.split("_")[0];
        return (tableId.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
