package com.tomgs.hadoop.test.yarn;

import java.util.Collection;
import java.util.Map;

/**
 * @author tangzhongyuan
 * @create 2018-11-01 15:49
 **/
public class YarnSubmitConditions {

    public String getMainClass() {
        return null;
    }

    public String getApplicationJar() {
        return null;
    }

    public Collection<String> getOtherArgs() {
        return null;
    }

    public String getJobName() {
        return null;
    }

    public String getSparkYarnJars() {
        return null;
    }

    public Object[] getAdditionalJars() {
        return new Object[0];
    }

    public Object[] getFiles() {
        return new Object[0];
    }

    public Map<String, String> getSparkProperties() {
        return null;
    }

    public String getYarnResourcemanagerAddress() {
        return null;
    }

    public String getSparkFsDefaultFS() {
        return null;
    }
}
