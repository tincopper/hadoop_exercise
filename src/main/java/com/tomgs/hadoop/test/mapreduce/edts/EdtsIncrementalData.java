package com.tomgs.hadoop.test.mapreduce.edts;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * @author tangzhongyuan
 * @create 2019-01-03 11:36
 **/
public class EdtsMetaData {

    private String table;

    private String type;

    private String keyIndexes;

    private String keyValues;

    private String rowValues;

    private long timestamp;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getKeyIndexes() {
        return keyIndexes;
    }

    public void setKeyIndexes(String keyIndexes) {
        this.keyIndexes = keyIndexes;
    }

    public String getKeyValues() {
        return keyValues;
    }

    public void setKeyValues(String keyValues) {
        this.keyValues = keyValues;
    }

    public String getRowValues() {
        return rowValues;
    }

    public void setRowValues(String rowValues) {
        this.rowValues = rowValues;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
