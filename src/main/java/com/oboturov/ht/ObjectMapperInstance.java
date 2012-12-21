package com.oboturov.ht;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author aoboturov
 */
public final class ObjectMapperInstance {
    private static final ObjectMapper ourInstance = new ObjectMapper();
    static {
    }

    public static ObjectMapper get() {
        return ourInstance;
    }

    private ObjectMapperInstance() {
    }
}
