package com.oboturov.ht;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author aoboturov
 */
public class ItemTest {

    @Test public void toString_json_serialization_test() {
        final String testType = "Some Test Type",
                testValue = "A value";
        final Item anItem = new Item(testType, testValue);
        assertEquals(anItem.toString(), "{\"type\":\""+testType+"\",\"value\":\""+testValue+"\"}");
    }

}
