package com.oboturov.ht;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author aoboturov
 */
public class User implements WritableComparable<User> {

    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
    }

    public static User readWritable(final DataInput dataInput) throws IOException {
        final User anUser = new User();
        anUser.name = dataInput.readUTF();
        return anUser;
    }

    public static void writeWritable(final User anUser, final DataOutput dataOutput) throws IOException {
        anUser.write(dataOutput);
    }

    @Override
    public int compareTo(final User rhs) {
        return this.name.compareTo(rhs.getName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        User user = (User) o;

        if (name != null ? !name.equals(user.name) : user.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "User[name='"+this.name+ "']";
    }
}
