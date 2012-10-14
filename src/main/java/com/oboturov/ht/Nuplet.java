package com.oboturov.ht;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author aoboturov
 */
public class Nuplet implements Writable {

    private User user;
    private Item item;
    private String keys;

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public Item getItem() {
        return item;
    }

    public void setItem(Item item) {
        this.item = item;
    }

    public String getKeys() {
        return keys;
    }

    public void setKeys(String keys) {
        this.keys = keys;
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException {
        user.write(dataOutput);
        item.write(dataOutput);
        dataOutput.writeUTF(keys);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException {
        this.user.readFields(dataInput);
        this.item.readFields(dataInput);
        this.keys = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Nuplet nuplet = (Nuplet) o;

        if (item != null ? !item.equals(nuplet.item) : nuplet.item != null) return false;
        if (keys != null ? !keys.equals(nuplet.keys) : nuplet.keys != null) return false;
        if (user != null ? !user.equals(nuplet.user) : nuplet.user != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = user != null ? user.hashCode() : 0;
        result = 31 * result + (item != null ? item.hashCode() : 0);
        result = 31 * result + (keys != null ? keys.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Nuplet[\n"+
                this.user.toString()+"\n"+
                this.item.toString()+"\n"+
                "keys='"+this.keys+"'\n"+
                "]";
    }
}
