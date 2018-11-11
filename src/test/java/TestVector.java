import com.alibaba.fastjson.JSONObject;
import com.tomgs.hadoop.test.util.JsonUtil;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    @Test
    public void test4() {
        Random random = new Random();
        int i = random.nextInt(100);
        System.out.println(i);
        int i1 = random.nextInt(200);
        System.out.println(i1);
    }

    @Test
    public void test5() throws InterruptedException {
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        final CountDownLatch cdl = new CountDownLatch(2);
        Thread t1 = new Thread(){
            @Override
            public void run() {
                System.out.println("====================");
                cdl.countDown();
            }
        };
        Thread t11 = new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                    System.out.println("-------------------");
                    File file = new File("D://test.txt");
                    if (!file.exists()) {
                        file.createNewFile();
                    }
                    cdl.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        executorService.submit(t11);
        executorService.submit(t1);

        cdl.await();

        executorService.shutdown();
        System.out.println(".................");
    }

    @Test
    public void test6() {
        int i = 0;
        List<Integer> list = new ArrayList<>();
        i++;
        list.add(i++);

        System.out.println(list.get(0));
    }
}
