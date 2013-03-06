package com.oboturov.ht.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * @author aoboturov
 */
public class MergeGroupedBags extends EvalFunc<DataBag> {
    @Override
    public DataBag exec(final Tuple input) throws IOException {
        if (input == null || input.size() == 0) return null;
        try {
            final DataBag bag = (DataBag)input.get(0);
            final DataBag mergedBags = new DefaultDataBag();
            for (final Tuple tuple : bag) {
                if (tuple == null || tuple.size() != 2) continue;
                final DataBag bagToMerge = (DataBag)tuple.get(1);
                mergedBags.addAll(bagToMerge);
            }
            return mergedBags;
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }
}
