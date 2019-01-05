package com.tomgs.hadoop.test.mapreduce.edts.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author tangzhongyuan
 * @create 2019-01-04 17:00
 **/
public class ProjectionDesc {

    private String name;

    private String channelId;
    private String channelName;

    private String sourceId;
    private String sourceDatabase;
    private String sourceTable;
    private List<Column> sourceColumns;

    private String targetId;
    private String targetDatabase;
    private String targetTable;
    private List<Column> targetColumns;

    private Properties sourceProperties;
    private Properties targetProperties;


    public ProjectionDesc() {
        sourceColumns = new ArrayList<>();
        targetColumns = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public void setSourceDatabase(String sourceDatabase) {
        this.sourceDatabase = sourceDatabase;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public List<Column> getSourceColumns() {
        return sourceColumns;
    }

    public void setSourceColumns(List<Column> sourceColumns) {
        this.sourceColumns = sourceColumns;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public void setTargetDatabase(String targetDatabase) {
        this.targetDatabase = targetDatabase;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public List<Column> getTargetColumns() {
        return targetColumns;
    }

    public void setTargetColumns(List<Column> targetColumns) {
        this.targetColumns = targetColumns;
    }

    public Properties getSourceProperties() {
        return sourceProperties;
    }

    public void setSourceProperties(Properties sourceProperties) {
        this.sourceProperties = sourceProperties;
    }

    public Properties getTargetProperties() {
        return targetProperties;
    }

    public void setTargetProperties(Properties targetProperties) {
        this.targetProperties = targetProperties;
    }
}
