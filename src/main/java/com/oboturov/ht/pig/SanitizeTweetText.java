package com.oboturov.ht.pig;

import com.google.common.collect.Lists;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.List;

/**
 * @author aoboturov
 */
public class SanitizeTweetText extends EvalFunc<String> {

    private static final String EMPTY_POST_INDICATION = "No Post Title";

    /**
     * @param text of tweet to be sanitized.
     * @return {@code null} if text was empty or trimmed string ifnot.
     */
    public static String sanitize(final String text) {
        if (text == null || EMPTY_POST_INDICATION.equals(text)) return null;
        final String trimmedText = text.trim();
        if (text.trim().isEmpty()) return null;
        return trimmedText;
    }

    @Override
    public String exec(final Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        try {
            final String text = (String)input.get(0);
            return sanitize(text);
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        return Lists.newArrayList(
                new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY)))
        );
    }
}
