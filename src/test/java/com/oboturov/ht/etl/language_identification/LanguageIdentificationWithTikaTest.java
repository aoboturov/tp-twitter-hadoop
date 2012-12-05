package com.oboturov.ht.etl.language_identification;

import org.apache.tika.language.LanguageIdentifier;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Set;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

/**
 * @author aoboturov
 */
public class LanguageIdentificationWithTikaTest {

    @Test
    public void loads_languages_from_tika_library() throws IOException {
        final Set<String> languages = LanguageIdentifier.getSupportedLanguages();
        assertNotNull(languages, "A set of supported languages required");
        assertFalse(languages.isEmpty(), "Some languages must be supported");
    }

}
