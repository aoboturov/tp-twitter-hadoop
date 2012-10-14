package com.oboturov.ht;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author aoboturov
 */
public class Item implements Writable {

    private String type;
    private String value;

    public Item(String type, String value) {
        this.type = type;
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.type);
        dataOutput.writeUTF(this.value);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException {
        this.type = dataInput.readUTF();
        this.value = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Item item = (Item) o;

        if (type != null ? !type.equals(item.type) : item.type != null) return false;
        if (value != null ? !value.equals(item.value) : item.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Item[type='"+this.type+"', value='"+this.value+"']";
    }
}
