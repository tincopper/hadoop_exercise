package com.tomgs.hadoop.test.mapreduce.edts;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author tangzhongyuan
 * @create 2019-01-05 10:22
 **/
public class EdtsDataContainer {

    private final Map<String, EdtsIncrementalData> dataContainer = new ConcurrentHashMap<>();

    public EdtsIncrementalData get(String key) {
        return dataContainer.get(key);
    }

    public void add(EdtsIncrementalData data) {
        if (!dataContainer.containsKey(data.getKeyValues())) {
            dataContainer.put(data.getKeyValues(), data);
            return;
        }
        EdtsIncrementalData originData = dataContainer.get(data.getKeyValues());
        if (data.getTimestamp() >= originData.getTimestamp()) {
            dataContainer.put(data.getKeyValues(), data);
        }
    }

    public void remove(EdtsIncrementalData data) {
        dataContainer.remove(data.getKeyValues());
    }

    public int size() {
        return dataContainer.size();
    }

    public void addAll(EdtsDataContainer container) {
        dataContainer.putAll(container.dataContainer);
    }

    public Collection<EdtsIncrementalData> listAll() {
        return dataContainer.values();
    }

    /**
     * 更新数据<br/>
     *
     * 更新的数据是保存最新的一条，所以直接添加覆盖即可<br/>
     * 如果对应的数据不存在历史数据，说明数据已经删除，则不添加
     * @param updateData 更新的数据
     */
    public void update(EdtsIncrementalData updateData) {
        if (dataContainer.containsKey(updateData.getKeyValues())) {
            dataContainer.put(updateData.getKeyValues(), updateData);
        }
    }
}
