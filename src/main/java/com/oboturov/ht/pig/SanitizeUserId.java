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
public class SanitizeUserId extends EvalFunc<String> {

    private static final String HTTP_TWITTER_COM = "http://twitter.com/";
    private static final String HTTP_WWW_TWITTER_COM = "http://www.twitter.com/";

    /**
     * @param userId to be transformed to a form @USER_ID.
     * @return canonized Twitter user ID.
     */
    public static String sanitize(final String userId) {
        if (userId == null) return null;
        if (userId.startsWith(HTTP_TWITTER_COM)) {
            return "@"+userId.substring(HTTP_TWITTER_COM.length());
        } else if (userId.startsWith(HTTP_WWW_TWITTER_COM)) {
            return "@"+userId.substring(HTTP_WWW_TWITTER_COM.length());
        }
        return null;
    }

    @Override
    public String exec(final Tuple input) throws IOException {
        if (input == null || input.size() == 0) return null;
        try {
            final String userId = (String)input.get(0);
            return sanitize(userId);
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
