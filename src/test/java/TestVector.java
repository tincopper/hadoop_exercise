import com.alibaba.fastjson.JSONObject;
import com.tomgs.hadoop.test.util.JsonUtil;
import org.junit.Test;

import java.util.*;

/**
 * @author tangzhongyuan
 * @create 2018-11-05 16:35
 **/
public class TestVector {

    @Test
    public void test1() {
        Vector<String> insertData = new Vector<>();
        Vector<String> originData = new Vector<>();

        insertData.add("abc");
        insertData.add("bcd");

        originData.add("abc");
        originData.add("bcd");
        originData.add("cde");

        System.out.println(originData.contains(insertData));
        System.out.println(insertData.contains("a"));

        for (ListIterator<String> iterator = originData.listIterator(); iterator.hasNext();) {
            String next = iterator.next();
            if (next.equals("abc")) {
                iterator.remove();
                iterator.add("123");
            }
            System.out.println(originData.contains(next));
            System.out.println(next);
        }

        /*for (Iterator<String> iterator = originData.iterator(); iterator.hasNext();) {
            String next = iterator.next(); //这里会报ConcurrentModificationException异常
            if (next.equals("abc")) {
                originData.add("123");
                iterator.remove();
            }
            System.out.println(originData.contains(next));
            System.out.println(next);
        }*/

        /*for (String originDatum : originData) {
            if (originDatum.equals("abc")) {
                originData.remove(originDatum);
            }
            System.out.println(originData.contains(originDatum));
            System.out.println(originDatum);
        }*/
    }

    @Test
    public void test2() {
        String json = "{\"I\":{\"columns\":21,\"field19\":\"value\",\"field17\":\"value\",\"field18\":\"value\",\"type\":0,\"field11\":\"value\",\"field12\":\"value\",\"field20\":\"value\",\"field1\":\"value\",\"field10\":\"value\",\"field15\":\"value\",\"field16\":\"value\",\"field13\":\"value\",\"field14\":\"value\",\"field7\":\"value\",\"field6\":\"value\",\"field9\":\"value\",\"field8\":\"value\",\"field3\":\"value\",\"field2\":\"value\",\"field5\":\"value\",\"table\":10,\"field4\":\"value\",\"timestamp\":\"2018-11-05\"},\"type\":0,\"table\":10,\"timestamp\":\"2018-11-05\"}";
        Map<String, Object> map = JsonUtil.convertJsonStrToMap(json);
        JSONObject i = (JSONObject)map.get("I");

        System.out.println(i.containsKey("field19"));
        System.out.println(i.containsValue("value"));

        System.out.println(map);
    }

    @Test
    public void test3() {
        Map<String, String> map = new HashMap<>();
        map.put("1", "a");
        map.put("2", "b");

        Map<String, String> map1 = new HashMap<>();
        map.put("1", "c");
        map.put("2", "d");

        map.putAll(map1);

        System.out.println(map);
    }
}
