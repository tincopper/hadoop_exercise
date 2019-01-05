package com.tomgs.hadoop.test.mapreduce.edts.entity;

import java.io.Serializable;

/**
 * @author tangzhongyuan
 * @create 2019-01-04 17:03
 **/
public class Column implements Serializable {
    private String columnName;
    /**
     * 列定义表达式
     */
    private String columnValue;
    private int columnType;
    private boolean isKey;
    private int index;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(String columnValue) {
        this.columnValue = columnValue;
    }

    public int getColumnType() {
        return columnType;
    }

    public void setColumnType(int columnType) {
        this.columnType = columnType;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setKey(boolean key) {
        isKey = key;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "Column{" +
                "columnName='" + columnName + '\'' +
                ", columnValue='" + columnValue + '\'' +
                ", columnType=" + columnType +
                ", isKey=" + isKey +
                ", index=" + index +
                '}';
    }
}