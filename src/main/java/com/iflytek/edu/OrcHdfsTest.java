package com.iflytek.edu;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

/**
 * created with idea
 * user:ztwu
 * date:2019/7/18
 * description
 */
public class OrcHdfsTest {

    public static void main(String[] args){
        OrcHdfsTest orcHdfsTest = new OrcHdfsTest();
        try {
            orcHdfsTest.wirteOrc();
//            orcHdfsTest.readOrc();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void wirteOrc() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.101:9000");
        TypeDescription schema = TypeDescription.createStruct()
                .addField("int_value", TypeDescription.createInt())
                .addField("long_value", TypeDescription.createLong())
                .addField("double_value", TypeDescription.createDouble())
                .addField("float_value", TypeDescription.createFloat())
                .addField("boolean_value", TypeDescription.createBoolean())
                .addField("string_value", TypeDescription.createString());

//        if(new File("data/my-file.orc").exists()){
//            new File("data/my-file.orc").deleteOnExit();
//        }
        Writer writer = OrcFile.createWriter(new Path("data/my-file.orc"),
                OrcFile.writerOptions(conf)
                        .setSchema(schema));

        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector intVector = (LongColumnVector) batch.cols[0];
        LongColumnVector longVector = (LongColumnVector) batch.cols[1];
        DoubleColumnVector doubleVector = (DoubleColumnVector) batch.cols[2];
        DoubleColumnVector floatColumnVector = (DoubleColumnVector) batch.cols[3];
        LongColumnVector booleanVector = (LongColumnVector) batch.cols[4];
        BytesColumnVector stringVector = (BytesColumnVector) batch.cols[5];

        for(int r=0; r < 100000; ++r) {
            int row = batch.size++;

            intVector.vector[row] = r;
            longVector.vector[row] = r;
            doubleVector.vector[row] = r;
            floatColumnVector.vector[row] = r;
            booleanVector.vector[row] =  r< 50000 ? 1 : 0;
            stringVector.setVal(row, UUID.randomUUID().toString().getBytes());

            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }

        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();
    }

    private void readOrc() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.101:9000");

        TypeDescription readSchema = TypeDescription.createStruct()
//        .addField("int_value", TypeDescription.createInt())
                .addField("long_value", TypeDescription.createLong())
                .addField("double_value", TypeDescription.createDouble())
                .addField("float_value", TypeDescription.createFloat())
                .addField("boolean_value", TypeDescription.createBoolean())
                .addField("string_value", TypeDescription.createString());


        Reader reader = OrcFile.createReader(new Path("data/my-file.orc"),
                OrcFile.readerOptions(conf));

        Reader.Options readerOptions = new Reader.Options(conf)
                .searchArgument(
                        SearchArgumentFactory
                                .newBuilder()
                                .between("long_value", PredicateLeaf.Type.LONG, 0L,1024L)
                                .build(),
                        new String[]{"long_value"}
                );

        RecordReader rows = reader.rows(readerOptions.schema(readSchema));

        VectorizedRowBatch batch = readSchema.createRowBatch();

        while (rows.nextBatch(batch)) {
//      LongColumnVector intVector = (LongColumnVector) batch.cols[0];
            LongColumnVector longVector = (LongColumnVector) batch.cols[0];
            DoubleColumnVector doubleVector  = (DoubleColumnVector) batch.cols[1];
            DoubleColumnVector floatVector = (DoubleColumnVector) batch.cols[2];
            LongColumnVector booleanVector = (LongColumnVector) batch.cols[3];
            BytesColumnVector stringVector = (BytesColumnVector)  batch.cols[4];

            for(int r=0; r < batch.size; r++) {
//        int intValue = (int) intVector.vector[r];
                long longValue = longVector.vector[r];
                double doubleValue = doubleVector.vector[r];
                double floatValue = (float) floatVector.vector[r];
                boolean boolValue = booleanVector.vector[r] != 0;
                String stringValue = new String(stringVector.vector[r], stringVector.start[r], stringVector.length[r]);

                System.out.println( longValue + ", " + doubleValue + ", " + floatValue + ", " + boolValue + ", " + stringValue);

            }
        }
        rows.close();
    }

}
