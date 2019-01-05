import com.tomgs.hadoop.test.mapreduce.edts.entity.Column;
import org.junit.Test;

import java.util.*;

/**
 * @author tangzhongyuan
 * @create 2019-01-04 17:25
 **/
public class TestColumn {

    @Test
    public void testSort() {
        List<Column> columns = new ArrayList<>();
        Column column1 = new Column();
        column1.setColumnName("id");
        column1.setColumnType(1);
        column1.setColumnValue("18");
        column1.setIndex(1);

        Column column2 = new Column();
        column2.setColumnName("name");
        column2.setColumnType(1);
        column2.setColumnValue("tomgs");
        column2.setIndex(2);

        columns.add(column1);
        columns.add(column2);
        //升序
        Collections.sort(columns, new Comparator<Column>() {
            @Override
            public int compare(Column o1, Column o2) {
                if (o1.getIndex() >= o2.getIndex()) {
                    return 1;
                }
                return -1;
            }
        });

        for (Column column : columns) {
            System.out.println(column);
        }
    }

    @Test
    public void testSubString() {
        String str = "ssss-sss-ss-";
        String substring = str.substring(0, str.lastIndexOf("-"));
        System.out.println(substring);
    }

    @Test
    public void testList() {
        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("1");

        System.out.println(list);

        Map<String, String> map = new HashMap<>();
        map.put("1", "2");
        map.put("1", "3");

        System.out.println(map);

        String remove = map.remove("3");

        System.out.println(System.currentTimeMillis());
    }
}
