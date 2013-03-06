package com.oboturov.ht.stage2;

import com.oboturov.ht.*;
import com.oboturov.ht.pig.TextTokenizer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @author aoboturov
 */
public class PhraseTokenizer {

    public static class PhraseTokenizerMap extends MapReduceBase implements Mapper<NullWritable, Nuplet, NullWritable, Nuplet> {

        private final static Logger logger = Logger.getLogger(PhraseTokenizerMap.class);

        enum Counters {
            PRODUCED_NUPLETS_WITH_ITEMS_ONLY, PRODUCED_NUPLETS_WITH_STEMMED_KEYWORDS, NUPLETS_WITH_NO_KEYWORD, LANGUAGE_NOT_SUPPORTED_AND_NO_ITEMS
        }

        @Override
        public void map(final NullWritable nothing, final Nuplet nuplet, final OutputCollector<NullWritable, Nuplet> output, final Reporter reporter) throws IOException {
            final String nupletKeywordType = nuplet.getKeyword().getType();
            final String nupletItemType = nuplet.getItem().getType();
            if (!KeyType.RAW_TEXT.equals(nupletKeywordType) && !KeyType.NULL.equals(nupletKeywordType)) {
                throw new RuntimeException("This Mapper is supposed to be used either with raw nuplets or without a keyword");
            }

            if (KeyType.NULL.equals(nupletKeywordType)) {
                // KeyType.NULL is just passed as is.
                reporter.incrCounter(Counters.NUPLETS_WITH_NO_KEYWORD, 1l);
                output.collect(NullWritable.get(), nuplet);
                return;
            }

            if (KeyType.RAW_TEXT.equals(nupletKeywordType)) {
                final List<String> tokens = TextTokenizer.tokenizeText(nuplet.getKeyword().getValue(), nuplet.getLang());

                // When language was not identified consider it as without a keyword.
                if (tokens.isEmpty()) {
                    reporter.incrCounter(Counters.PRODUCED_NUPLETS_WITH_ITEMS_ONLY, 1l);
                    nuplet.setKeyword(Keyword.NO_KEYWORD);
                    output.collect(NullWritable.get(), nuplet);
                    return;
                }

                // When language was detected.
                for (final String token : tokens) {
                    final Nuplet newNuplet = new Nuplet();
                    newNuplet.setUser(nuplet.getUser());
                    newNuplet.setLang(nuplet.getLang());
                    newNuplet.setKeyword(new Keyword(KeyType.STEMMED_ENTITY, token));
                    newNuplet.setItem(nuplet.getItem());
                    // Produce new nuplet with updated key value.
                    output.collect(NullWritable.get(), newNuplet);
                    reporter.incrCounter(Counters.PRODUCED_NUPLETS_WITH_STEMMED_KEYWORDS, 1l);
                }
            }
        }
    }

}
