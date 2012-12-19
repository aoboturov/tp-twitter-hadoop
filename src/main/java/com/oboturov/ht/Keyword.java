package com.oboturov.ht;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author aoboturov
 */
public class Keyword implements WritableComparable<Keyword>{

    private String type;
    private String value;

    public Keyword(String type, String value) {
        this.type = type;
        this.value = value;
    }

    private Keyword() {}

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

    public static void writeWritable(final Keyword aKeyword, final DataOutput dataOutput) throws IOException {
        aKeyword.write(dataOutput);
    }

    public static Keyword readWritable(final DataInput dataInput) throws IOException {
        final Keyword aKeyword = new Keyword();
        aKeyword.readFields(dataInput);
        return aKeyword;
    }

    @Override
    public int compareTo(final Keyword rhs) {
        int res = this.type.compareTo(rhs.getType());
        if ( res != 0) {
            return res;
        }
        return this.value.compareTo(rhs.getValue());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Keyword keyword = (Keyword) o;

        if (type != null ? !type.equals(keyword.type) : keyword.type != null) return false;
        if (value != null ? !value.equals(keyword.value) : keyword.value != null) return false;

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
        return "Keyword[type='"+this.type+"', value='"+this.value+"']";
    }
}
