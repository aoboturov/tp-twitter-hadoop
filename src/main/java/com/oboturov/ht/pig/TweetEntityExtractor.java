package com.oboturov.ht.pig;

import com.google.common.collect.Lists;
import com.twitter.Extractor;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.List;

/**
 * @author aoboturov
 */
public class TweetEntityExtractor extends EvalFunc<Tuple> {

    private final ThreadLocal<Extractor> extractor = new ThreadLocal<Extractor>() {
        @Override
        protected Extractor initialValue() {
            return new Extractor();
        }
    };

    public static String extractRawTextFromTweetPost(final String post, final List<Extractor.Entity> entities) {
        int left = 0;
        final StringBuilder rawText = new StringBuilder(post.length());
        for (Extractor.Entity entity : entities) {
            rawText.append(post.substring(left, entity.getStart()));
            left = entity.getEnd();
        }
        rawText.append(post.substring(left, post.length()));
        return rawText.toString();
    }

    @Override
    public Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.size() == 0) return null;
        try {
            final String text = (String)input.get(0);
            final List<Extractor.Entity> entities = extractor.get().extractEntitiesWithIndices(text);
            final String rawText = extractRawTextFromTweetPost(text, entities);

            final Tuple result = new DefaultTuple();
            final DataBag mentions = new DefaultDataBag();
            final DataBag hashes = new DefaultDataBag();
            final DataBag urls = new DefaultDataBag();
            // Generate one nuplet per Item.
            for (Extractor.Entity entity : entities) {
                final Tuple tuple = new DefaultTuple();
                switch (entity.getType()) {
                    case CASHTAG:
                        // Do not support CASHTAGs in analysis phase.
                        continue;
                    case HASHTAG:
                        tuple.append("#"+entity.getValue());
                        hashes.add(tuple);
                        break;
                    case MENTION:
                        tuple.append("@"+entity.getValue());
                        mentions.add(tuple);
                        break;
                    case URL:
                        tuple.append(entity.getValue());
                        urls.add(tuple);
                        break;
                }
            }
            result.append(mentions);
            result.append(hashes);
            result.append(urls);
            result.append(rawText);
            return result;
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
