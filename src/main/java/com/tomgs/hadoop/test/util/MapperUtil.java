package com.tomgs.hadoop.test.util;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author tangzhongyuan
 * @create 2019-01-04 15:24
 **/
public class MapperUtil {

    /**
     * 获得输入文件路径
     */
    public static String getFilePath(Context context) throws IOException {
        InputSplit split = context.getInputSplit();
        Class<? extends InputSplit> splitClass = split.getClass();
        FileSplit fileSplit = null;
        if (splitClass.equals(FileSplit.class)) {
            fileSplit = (FileSplit) split;
        } else if (splitClass.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
            try {
                Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
                //设置访问权限  true：不需要访问权限检测直接使用  false：需要访问权限检测
                getInputSplitMethod.setAccessible(true);
                fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        return fileSplit.getPath().toUri().getPath();
    }

}
