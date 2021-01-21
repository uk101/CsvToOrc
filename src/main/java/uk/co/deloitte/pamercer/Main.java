package uk.co.deloitte.pamercer;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class Main {

    private static final String COMMA_DELIMITER = ",";

    public static void main(String[] args) throws IOException {
        final String home = System.getProperty("user.home");
        try (BufferedReader br = new BufferedReader(new FileReader(String.format("%s/code/testdata/logfile.txt", home)))) {

            TypeDescription schema = TypeDescription.fromString("struct<ts:timestamp,n1:int,n2:int,n3:int,source:string,action:string,message:string>");
            OrcFile.WriterOptions opts = OrcFile.writerOptions(new Configuration()).setSchema(schema);
            VectorizedRowBatch batch = schema.createRowBatch();
            TimestampColumnVector ts = (TimestampColumnVector) batch.cols[0];
            LongColumnVector n1 = (LongColumnVector) batch.cols[1];
            LongColumnVector n2 = (LongColumnVector) batch.cols[2];
            LongColumnVector n3 = (LongColumnVector) batch.cols[3];
            BytesColumnVector source = (BytesColumnVector) batch.cols[4];
            BytesColumnVector action = (BytesColumnVector) batch.cols[5];
            BytesColumnVector message = (BytesColumnVector) batch.cols[6];

            try (Writer writer = OrcFile.createWriter(new Path(orcFilePath(home)), opts)) {
                br.lines().forEach(line -> {
                    String fields[] = line.split(COMMA_DELIMITER);
                    int row = 0;
                    if(fields.length>=7) {
                        row = batch.size++;
                        ts.time[row] = getTimeStamp(fields[0] + fields[1]);
                        n1.vector[row] = Long.parseLong(fields[2].trim());
                        n2.vector[row] = Long.parseLong(fields[3].trim());
                        n3.vector[row] = Long.parseLong(fields[5].trim());
                        source.setVal(row, fields[4].trim().getBytes());
                        action.setVal(row, fields[6].trim().getBytes());
                        message.setVal(row, getMessage(fields));
                        if(batch.size == batch.getMaxSize()) {
                            try {
                                writer.addRowBatch(batch);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            batch.reset();
                        }
                    }
                });
                if(batch.size > 0) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
        }
    }

    private static byte[] getMessage(String[] fields) {
        final byte[] empty = new byte[0];
        byte[] message = empty;
        if(fields.length>7) {
            final String delim = " ";
            try {
                String msg = fields.length == 8 ? fields[7] :
                        String.join(delim, Arrays.asList(Arrays.copyOfRange(fields, 7, fields.length - 1)));
                message = msg.trim().getBytes();
            } catch (NullPointerException e) {
                System.out.println("Array copy error");
                System.out.println(fields.length);
                e.printStackTrace();
            }
        }
        return message;
    }

    private static long getTimeStamp(String dateToParse) {
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:ms");
        try {
            return dateFormat.parse(dateToParse).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            return 0;
        }
    }

    private static String orcFilePath(String home) {
        String orcFilePath = String.format("%s/code/testdata/logfile.orc", home);
        File f = new File(orcFilePath);
        if(f.exists() && !f.isDirectory()) {
            f.delete();
        }
        return orcFilePath;
    }
}