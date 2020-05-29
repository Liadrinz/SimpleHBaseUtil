package hbase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    private static final HBaseUtil hBaseUtil = new HBaseUtil();
    // 模拟原始学生表
    private static final Map<String, String[]> students = new HashMap<String, String[]>();
    static {
        //                 主键                                   数据行
        students.put("2015001", new String[] {"2015001", "Zhangsan", "male", "23"});
        students.put("2015002", new String[] {"2015002", "Mary", "female", "22"});
        students.put("2015003", new String[] {"2015003", "Lisi", "male", "24"});
    }
    // 模拟原始课程表
    private static final Map<String, String[]> courses = new HashMap<String, String[]>();
    static {
        //               主键                            数据行
        courses.put("123001", new String[] {"123001", "Math", "2.0"});
        courses.put("123002", new String[] {"123002", "Computer Science", "5.0"});
        courses.put("123003", new String[] {"123003", "English", "3.0"});
    }
    // 模拟原始选课表
    private static final Map<String[], String[]> studentsCourses = new HashMap<String[], String[]>();
    static {
        //                                       外键                                      数据行
        studentsCourses.put(new String[] {"2015001", "123001"}, new String[] {"2015001", "123001", "86"});
        studentsCourses.put(new String[] {"2015001", "123003"}, new String[] {"2015001", "123003", "69"});
        studentsCourses.put(new String[] {"2015002", "123002"}, new String[] {"2015002", "123002", "77"});
        studentsCourses.put(new String[] {"2015002", "123003"}, new String[] {"2015002", "123003", "99"});
        studentsCourses.put(new String[] {"2015003", "123001"}, new String[] {"2015003", "123001", "98"});
        studentsCourses.put(new String[] {"2015003", "123002"}, new String[] {"2015003", "123002", "95"});
    }
    // HBase列族和列
    private static final String[] fields = {
            "info:No", "info:name", "info:sex", "info:age",
            "course:No", "course:name", "course:credit", "course:score"
    };
    public static void main(String[] args) {
        createAndMigrate();
        System.out.println("==============================");
        System.out.println("After Creation and Migration:");
        printScanEachColumn();

        doSomeModifies();
        System.out.println("==============================");
        System.out.println("After Some Modifications:");
        printScanEachColumn();

        doSomeDelete();
        System.out.println("==============================");
        System.out.println("After Some Deletions");
        printScanEachColumn();
    }
    private static void createAndMigrate() {
        hBaseUtil.createTable("student", new String[] {"info", "course"});
        int rowNum = 0;
        for (String[] key : studentsCourses.keySet()) {
            String[] stu = students.get(key[0]);
            String[] cour = courses.get(key[1]);
            String[] sc = studentsCourses.get(key);
            hBaseUtil.addRecord("student", "r" + rowNum++, fields, new String[]{
                    stu[0], stu[1], stu[2], stu[3],
                    cour[0], cour[1], cour[2], sc[2]
            });
        }
    }
    private static void printScanEachColumn() {
        for (String field : fields) {
            List<String> scanResult = hBaseUtil.scanColumn("student", field);
            System.out.println(field + ": " + scanResult);
        }
    }
    private static void doSomeModifies() {
        // 把r2的成绩改为0
        // 把r3的成绩改为100
        hBaseUtil.modifyData("student", "r2", "course:score", "0");
        hBaseUtil.modifyData("student", "r3", "course:score", "100");
        // 把Zhangsan改成Wangwu
        hBaseUtil.modifyData("student", "r4", "info:name", "Wangwu");
        hBaseUtil.modifyData("student", "r5", "info:name", "Wangwu");
    }
    private static void doSomeDelete() {
        // 删除r3和r4的记录
        hBaseUtil.deleteRow("student", "r3");
        hBaseUtil.deleteRow("student", "r4");
    }
}
