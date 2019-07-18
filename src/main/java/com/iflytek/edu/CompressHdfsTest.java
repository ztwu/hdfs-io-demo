package com.iflytek.edu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * created with idea
 * user:ztwu
 * date:2019/7/18
 * description
 */
public class CompressHdfsTest {

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.101:9000");

        /**
         压缩操作
         **/
        /*-----gzip格式目录压缩-----
        org.apache.hadoop.io.compress.GzipCodec  /vehicle/pred/alarm/2018/07/13  .log .gz

          ---压缩单个文件----
        org.apache.hadoop.io.compress.GzipCodec  /vehicle/pred/alarm/2018/07/14/1531582800000-192.168.3.73.log    .log   .gz*/

        /**
         *解压操作
         **/
        /*org.apache.hadoop.io.compress.GzipCodec  /vehicle/alarm/2018/07/16  .gz .log

        -------lzo格式--------
        *//*压缩操作*//*
        com.hadoop.compression.lzo.LzopCodec /vehicle/alarm/2018/07/10  .log .lzo

        *//*解压操作*//*
        com.hadoop.compression.lzo.LzopCodec /vehicle/alarm/2018/07/10   .lzo .log*/
        String[] uargs = new String[]{"uncompress","org.apache.hadoop.io.compress.GzipCodec","/user/root/data/my-file.gz",".gz",".orc"};
//        String[] uargs = new String[]{"compress","org.apache.hadoop.io.compress.GzipCodec","/user/root/data/my-file.orc",".orc",".gz"};
        String opt_type = uargs[0];
        String code_class = uargs[1];
        String source_dir = uargs[2];
        String sourceType = uargs[3];
        String targetType = uargs[4];
        if ("compress".equals(opt_type)) {
            compress(code_class, source_dir, conf, sourceType, targetType);
        } else if ("uncompress".equals(opt_type)) {
            uncompress(code_class, source_dir, conf, sourceType, targetType);
        }

    }

    /**
     * 压缩
     *
     * @param code_class
     * @param source_dir
     * @param sourceType
     * @param conf
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public static void compress(String code_class, String source_dir, Configuration conf, String sourceType, String targetType) throws ClassNotFoundException, IOException {
        Class<?> codeClass = Class.forName(code_class);
        FileSystem fs = FileSystem.get(conf);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codeClass, conf);

        if (fs.isDirectory(new Path(source_dir))) {
            List<Path> list = listFiles(fs, source_dir);
            list.forEach(path -> {
//            Path path = new Path(source_dir);
                compressSingleFile(path, fs, codec, conf, sourceType, targetType);
            });
        } else {
            compressSingleFile(new Path(source_dir), fs, codec, conf, sourceType, targetType);
        }

    }

    private static void compressSingleFile(Path path, FileSystem fs, CompressionCodec codec, Configuration conf, String sourceType, String targetType) {
        if (path.getName().indexOf(sourceType) > 0) {
            FSDataInputStream input = null;
            CompressionOutputStream compress_out = null;
            try {

                input = fs.open(path);
                System.out.println("开始压缩------" + path.toUri());
                FSDataOutputStream output = fs.create(new Path(path.toUri().toString().replace(sourceType, targetType)), () -> System.out.println("*"));
                compress_out = codec.createOutputStream(output);
                IOUtils.copyBytes(input, compress_out, conf);
                System.out.println(path.toUri() + "--------压缩完成");
                fs.delete(path, false);
                System.out.println("------------------------------------------------开始压缩下一个文件-------------------------------------------------------");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(input);
                IOUtils.closeStream(compress_out);
            }
        }
    }


    private static List<Path> listFiles(FileSystem fs, String source_dir) {

        List<Path> list = new ArrayList<>();
        Path path = new Path(source_dir);
        try {
            if (fs.isDirectory(path)) {
                RemoteIterator<LocatedFileStatus> ril = fs.listFiles(path, true);
                while (ril.hasNext()) {
                    list.add(ril.next().getPath());
                }
            } else {
                list.add(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 解压缩
     *
     * @param decode_class
     * @param source_dir
     * @param sourceType
     * @param targetType
     * @param conf
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public static void uncompress(String decode_class, String source_dir, Configuration conf, String sourceType, String targetType) throws IOException, ClassNotFoundException {
        Class<?> decodeClass = null;
        decodeClass = Class.forName(decode_class);
        System.out.println("load lzo class completed: " + decodeClass.getName());
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(decodeClass, conf);

        FileSystem fs = FileSystem.get(conf);
        if (fs.isDirectory(new Path(source_dir))) {
            List<Path> list = listFiles(fs, source_dir);
            list.forEach(path -> {
//            Path path = new Path(source_dir);
                uncompressSingleFile(path, fs, codec, conf, sourceType, targetType);
            });
        } else {
            uncompressSingleFile(new Path(source_dir), fs, codec, conf, sourceType, targetType);
        }

    }


    public static void uncompressSingleFile(Path path, FileSystem fs, CompressionCodec codec, Configuration conf, String sourceType, String targetType) {
        if (path.getName().indexOf(sourceType) > 0) {
            FSDataInputStream input = null;
            OutputStream output = null;
            try {
                input = fs.open(path);
                System.out.println("开始解压------" + path.toUri());
                CompressionInputStream codec_input = codec.createInputStream(input);
                output = fs.create(new Path(path.toUri().toString().replace(sourceType, targetType)));
                System.out.println(codec_input.toString() + "--" + output.toString());
                IOUtils.copyBytes(codec_input, output, conf);
                System.out.println(path.toUri() + "--------解压完成");
                System.out.println("------------------------------------------------开始解压下一个文件-------------------------------------------------------");
                fs.delete(path, false);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(input);
                IOUtils.closeStream(output);
            }
        }
    }

}
