package com.iflytek.edu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/8/14
 * Time: 17:20
 * Description
 */

public class HdfsTest {

    public static void createFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/project/edu_edcc/ztwu2/temp/testHDFS.txt");
        fs.create(path);
        fs.close();
    }

    public static void deleteFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.101:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/ztwu/hadoop/data/2017-08-14");
        fs.deleteOnExit(path);
        fs.close();
    }

    public static void writeFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.101:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/hdfs/data/test.txt");
        FSDataOutputStream out = fs.create(path);
        out.writeUTF("da jia hao,cai shi zhen de hao!");
        fs.close();
    }

    public static void readFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.101:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/ztwu/hadoop/data/test.txt");
        if(fs.exists(path)){
            FSDataInputStream is = fs.open(path);
            FileStatus status = fs.getFileStatus(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(is,"UTF-8"));
            String line;
            while((line = br.readLine())!=null){
                System.out.println(line);
            }
            is.close();
        }
    }

    public static void copyFromLocalFile () throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.101:9000");
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path("C:/Users/admin/Desktop/res_type.txt");
        Path dst = new Path("/user/hdfs/");
        fs.copyFromLocalFile(src, dst);
        fs.close();
    }

    public static void main(String[] args) throws IOException {
        createFile();
//        deleteFile();
//        writeFile();
//        readFile();
//        copyFromLocalFile();
    }

}
