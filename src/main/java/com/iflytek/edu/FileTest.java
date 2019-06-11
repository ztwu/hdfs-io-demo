package com.iflytek.edu;

import java.io.*;
import java.nio.charset.Charset;

/**
 * created with idea
 * user:ztwu
 * date:2019/5/21
 * description
 *
 * nputStream 和 OutStream  字节流
 * InputStreamReader 和 OutStreamWriter  字符流（桥梁），可以设置字符编码
 * BufferedReader 和 BufferedWriter  把字节或者字符放入缓冲区，提高读写速度
 * BufferedRead提供了一套缓冲机制，读取文件时先在内存中建立一块缓冲区，然后读取文件填满缓冲区，
 * 然后在缓冲区中对文件进行实际的操作，当缓冲区读完后调用read方法继续填充缓冲区；
 *
 * 2.要先用字节流来读取或者写入文件，因为可以避免中文乱码。
 * 3.面对存入文件的数据类型不同选取不同的流方式：字节流，字符流，对象流。
 * 4.在读写的时候，要把流放入缓冲区Buffer里，这样可以加快读写速度。
 * 5.可以总结出一般思路：FileInputStream(字节流)--->InputStreamWriter(字符流)--->BufferedWriter(缓冲池).
 * 6.BufferedWriter bw = new BufferedWriter(new InputStreamWriter(new FileInputStream(file, true)));
 *
 */
public class FileTest {

    public static void main(String[] args) {
        FileTest t = new FileTest();
//        t.write2file("测试写入");
        t.readfile();
    }

    public void write2file(String cbuf) {
        File file = null;//首先定义文件类
        OutputStream os = null;//定义字节流
        OutputStreamWriter osw = null;//OutputStreamWriter是字节流通向字符流的桥梁。
        BufferedWriter bw = null;//定义缓冲区

        try {
            file = new File("data/test.txt"); //新建文件对象
            //从文件系统中的某个文件中获取字节
            os = new FileOutputStream(file, true); //true是append设为允许，即可以在原文件末端追加。
            //将字节流转换成字符流
            osw = new OutputStreamWriter(os);
            //把接收到的字符流放入缓冲区，提高读写速度。
            bw = new BufferedWriter(osw);

            //将字符串以流的形式写入文件
            bw.write(cbuf);
        } catch (FileNotFoundException e) {
            System.out.println("找不到指定文件");
        } catch (IOException e) {
            System.out.println("写入文件错误");
        } finally {
            try {
                //关闭文件放到finally里，无论读取是否成功，都要把流关闭。
                //关闭的顺序：最后开的先关闭，栈的先进后出原理。
                bw.close();
                osw.close();
                os.close();
            } catch (IOException e) {
                System.out.println("文件流无法关闭");
            }
        }
    }

    public void readfile(){
        BufferedReader reader=null;
        try {
            String str=null;
            String filePath="data/test.txt";//文件使用UTF-8编码
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
            str=reader.readLine();
            System.out.println("使用默认编码:"+str);
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(filePath), Charset.forName("GBK")));
            str=reader.readLine();
            System.out.println("使用GBK编码:"+str);
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(filePath),Charset.forName("UTF-8")));
            str=reader.readLine();
            System.out.println("使用UTF-8编码:"+str);
            reader=new BufferedReader(new FileReader(filePath));
            str=reader.readLine();
            System.out.println("使用FileReader读取："+str);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
