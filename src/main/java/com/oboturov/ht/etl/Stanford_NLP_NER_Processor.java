package com.oboturov.ht.etl;

import com.oboturov.ht.KeyType;
import com.oboturov.ht.Nuplet;
import com.oboturov.ht.User;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.util.CoreMap;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * @author aoboturov
 */
public class Stanford_NLP_NER_Processor {

    public static class Map extends MapReduceBase implements Mapper<User, Nuplet, User, Nuplet> {

        private static final Logger logger = Logger.getLogger(Map.class);

        public static final String NER_CLASSIFIER_EN = "classifiers/english.muc.7class.distsim.crf.ser.gz";
        public static final String NER_CLASSIFIER_EN_PROPERTIES_STREAM = "/classifiers/english.muc.7class.distsim.prop";

        private static final String NER_CLASSIFIER_LOAD_ERROR_MSG = "NER classifier was not loaded: " + NER_CLASSIFIER_EN;
        private static final String NER_CLASSIFIER_PROPERTIES_LOAD_ERROR_MSG =
                "NER classifier properties were not loaded: " + NER_CLASSIFIER_EN_PROPERTIES_STREAM;

        final CRFClassifier<CoreMap> classifier = new CRFClassifier<CoreMap>(new Properties());

        public Map() throws NullPointerException {
            this(true);
        }

        Map(final boolean loadFromJar) throws NullPointerException {
            final InputStream modelPropertiesStream = getClass().getResourceAsStream(NER_CLASSIFIER_EN_PROPERTIES_STREAM);
            if ( modelPropertiesStream == null) {
                logger.error(NER_CLASSIFIER_PROPERTIES_LOAD_ERROR_MSG);
                throw new NullPointerException(NER_CLASSIFIER_PROPERTIES_LOAD_ERROR_MSG);
            }
            final Properties modelProperties = new Properties();
            try {
                modelProperties.load(modelPropertiesStream);
            } catch (IOException e) {
                logger.error(NER_CLASSIFIER_PROPERTIES_LOAD_ERROR_MSG, e);
                throw new NullPointerException(NER_CLASSIFIER_PROPERTIES_LOAD_ERROR_MSG);
            } finally {
                try {
                    modelPropertiesStream.close();
                } catch (IOException e) {
                    logger.error(e);
                }
            }
            if ( loadFromJar) {
                classifier.loadJarClassifier(NER_CLASSIFIER_EN, modelProperties);
            } else {
                try {
                    classifier.loadClassifier(NER_CLASSIFIER_EN, modelProperties);
                } catch (IOException e) {
                    logger.error(NER_CLASSIFIER_LOAD_ERROR_MSG, e);
                    throw new NullPointerException(NER_CLASSIFIER_LOAD_ERROR_MSG);
                } catch (ClassNotFoundException e) {
                    logger.error(NER_CLASSIFIER_LOAD_ERROR_MSG, e);
                    throw new NullPointerException(NER_CLASSIFIER_LOAD_ERROR_MSG);
                }
            }
        }

        @Override
        public void map(final User user, final Nuplet nuplet, final OutputCollector<User, Nuplet> outputCollector, final Reporter reporter) throws IOException {
            if (KeyType.PLAIN_TEXT.equals(nuplet.getKeyword().getType())) {
                final List<List<CoreMap>> classification = classifier.classify(nuplet.getKeyword().getValue());

                for ( final List<CoreMap> coreMaps : classification) {
                    for ( final CoreMap coreMap : coreMaps) {
                        // TODO: provide implementation.
                        logger.debug(coreMap);
                    }
                }
            } else {
                // pass through all other nuplet types.
                outputCollector.collect(user, nuplet);
            }
        }
    }

}
