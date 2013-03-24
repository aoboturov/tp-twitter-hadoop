package com.oboturov.ht.crossjoin;

import datafu.pig.bags.BagConcat;
import org.apache.pig.builtin.DIFF;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

import java.io.*;
import java.util.zip.GZIPOutputStream;

/**
 * @author aoboturov
 */
public class InMemoryCrossJoin {

    public static final int USER_NAME = 0;
    public static final int LIST = 1;

    public static double threshold = 0.001;

    public static void main(final String args[]) throws Exception {
        final String schema = "user_id_l:chararray, values_l:bag {T: tuple(item:chararray)}";
        final PigStorageReaderSimplified storageReader = new PigStorageReaderSimplified(schema);

        final OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(args[1]));

        final DIFF diff = new DIFF();
        final BagConcat bagConcat = new BagConcat();

        // Read all the tuples from storage.
        String leftLine = null, rightLine = null;
        final LineNumberReader leftLineNumberReader = new LineNumberReader(
                new FileReader(args[0]));
        while ( (leftLine = leftLineNumberReader.readLine()) != null) {
            final Tuple leftTuple = storageReader.getNext(leftLine);
            final LineNumberReader rightLineNumberReader = new LineNumberReader(
                    new FileReader(args[0]));
            System.err.println(leftLine);

            while ( (rightLine = rightLineNumberReader.readLine()) != null) {
                final Tuple rightTuple = storageReader.getNext(rightLine);

                final Tuple diffTuple = new DefaultTuple();
                diffTuple.append(leftTuple.get(LIST));
                diffTuple.append(rightTuple.get(LIST));
                final long diffSize = diff.exec(diffTuple).size();
                final long unionSize = bagConcat.exec(diffTuple).size();
                double sim = 1.0 - ((double)diffSize)/((double)unionSize);
                if (sim > threshold && sim > 0.0 && !leftTuple.get(USER_NAME).equals(rightTuple.get(USER_NAME))) {
                    final StringBuilder sb = new StringBuilder();
                    sb.append(leftTuple.get(USER_NAME)).append("\t")
                            .append(rightTuple.get(USER_NAME)).append("\t")
                            .append(String.format("%.3f", sim)).append("\n");
                    outputStream.write(sb.toString().getBytes("UTF-8"));
                }
                outputStream.flush();
            }
        }
    }

}
