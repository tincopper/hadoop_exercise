package com.tomgs.hadoop.test.mapreduce.join.demo1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author tangzhongyuan
 * @create 2018-11-05 14:18
 **/
public class FlagDataType implements WritableComparable<FlagDataType> {

    private String info;
    private int flag;

    @Override
    public int compareTo(FlagDataType o) {
        return this.flag - o.getFlag();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(info);
        out.writeInt(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.info = in.readUTF();
        this.flag = in.readInt();
    }

    @Override
    public boolean equals(Object obj) {
        if(null == obj) {
            return false;
        }
        if(this == obj) {
            return true;
        }
        if(obj instanceof FlagDataType) {
            FlagDataType o = (FlagDataType) obj;
            if(this.info.equals(o.getInfo()) && this.flag == o.getFlag()) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }
}
