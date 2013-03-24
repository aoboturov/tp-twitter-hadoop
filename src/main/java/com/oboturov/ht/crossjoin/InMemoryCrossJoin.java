package com.oboturov.ht.crossjoin;

import datafu.pig.bags.BagConcat;
import org.apache.pig.builtin.DIFF;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.LineNumberReader;
import java.util.ArrayList;

/**
 * @author aoboturov
 */
public class InMemoryCrossJoin {

    public static final int USER_NAME = 0;
    public static final int LIST = 1;

    public static double threshold = 0.001;

    public static void main(final String args[]) throws Exception {
        final LineNumberReader lineNumberReader = new LineNumberReader(
                new FileReader(args[0]));
        final String schema = "user_id_l:chararray, values_l:bag {T: tuple(item:chararray)}";
        final PigStorageReaderSimplified storageReader = new PigStorageReaderSimplified(schema);

        final ArrayList<Tuple> tuples = new ArrayList<Tuple>(2000000);
        // Read all the tuples from storage.
        String line = null;
        while ( (line = lineNumberReader.readLine()) != null) {
            tuples.add(storageReader.getNext(line));
        }
        final FileWriter fileWriter = new FileWriter(args[1]);
        final DIFF diff = new DIFF();
        final BagConcat bagConcat = new BagConcat();
        // Perform the Cross-Join.
        for (int lCnt = 0; lCnt < tuples.size(); lCnt++) {
            final Tuple leftTuple = tuples.get(lCnt);
            for (int rCnt = 0; rCnt < tuples.size(); rCnt++) {
                final Tuple rightTuple = tuples.get(rCnt);
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
                    fileWriter.write(sb.toString());
                    fileWriter.flush();
                }
            }
        }
        fileWriter.close();
    }

}
