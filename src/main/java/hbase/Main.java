package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Main {
    private static String[] fields = {
            "info:No", "info:name", "info:sex", "info:age",
            "course:No", "course:name", "course:credit", "course:score"
    };
    private static String[][] students = {
            new String[] {"2015001", "Zhangsan", "male", "23"},
            new String[] {"2015002", "Mary", "female", "22"},
            new String[] {"2015003", "Lisi", "male", "24"},
    };
    private static String[][] courses = {
            new String[] {"123001", "Math", "2.0"},
            new String[] {"123002", "Computer Science", "5.0"},
            new String[] {"123003", "English", "3.0"},
    };
    private static String[][] studentsCourses = {
            new String[] {"2015001", "123001", "86"},
            new String[] {"2015001", "123003", "69"},
            new String[] {"2015002", "123002", "77"},
            new String[] {"2015002", "123003", "99"},
            new String[] {"2015003", "123001", "98"},
            new String[] {"2015003", "123002", "95"}
    };
    public static void main(String[] args) {
        HBaseUtil hBaseUtil = new HBaseUtil();
        hBaseUtil.createTable("student", new String[] {"info", "course"});
        int rowNum = 0;
        for (String[] studentCourse : studentsCourses) {
            String[] stu = new String[4], cour = new String[4];
            for (String[] student : students) {
                if (studentCourse[0].equals(student[0])) {
                    stu = student;
                    break;
                }
            }
            for (String[] course : courses) {
                if (studentCourse[1].equals(course[0])) {
                    cour = course;
                    break;
                }
            }
            hBaseUtil.addRecord("student", "r" + rowNum++, fields, new String[]{
                    stu[0], stu[1], stu[2], stu[3],
                    cour[0], cour[1], cour[2], studentCourse[2]
            });
        }
        List<String> studentNames = hBaseUtil.scanColumn("student", "info:name");
        List<String> courseNames = hBaseUtil.scanColumn("student", "course:name");
        List<String> scores = hBaseUtil.scanColumn("student", "course:score");
        System.out.println(studentNames);
        System.out.println(courseNames);
        System.out.println(scores);
    }
}
