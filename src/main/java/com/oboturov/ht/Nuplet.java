package com.oboturov.ht;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author aoboturov
 */
public class Nuplet implements WritableComparable<Nuplet> {

    private User user;
    private Item item;
    private Keyword keyword;
    // Meta-data.
    private String lang = "";

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

    public Keyword getKeyword() {
        return keyword;
    }

    public void setKeyword(Keyword keyword) {
        this.keyword = keyword;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException {
        User.writeWritable(user, dataOutput);
        Item.writeWritable(item, dataOutput);
        Keyword.writeWritable(keyword, dataOutput);
        dataOutput.writeUTF(lang);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException {
        this.user = User.readWritable(dataInput);
        this.item = Item.readWritable(dataInput);
        this.keyword = Keyword.readWritable(dataInput);
        this.lang = dataInput.readUTF();
    }

    @Override
    public int compareTo(final Nuplet rhs) {
        int res;
        res = this.user.compareTo(rhs.getUser());
        if ( res != 0) {
            return res;
        }
        res = this.item.compareTo(rhs.getItem());
        if ( res != 0) {
            return res;
        }
        return this.keyword.compareTo(rhs.getKeyword());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Nuplet nuplet = (Nuplet) o;

        if (item != null ? !item.equals(nuplet.item) : nuplet.item != null) return false;
        if (keyword != null ? !keyword.equals(nuplet.keyword) : nuplet.keyword != null) return false;
        if (user != null ? !user.equals(nuplet.user) : nuplet.user != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = user != null ? user.hashCode() : 0;
        result = 31 * result + (item != null ? item.hashCode() : 0);
        result = 31 * result + (keyword != null ? keyword.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Nuplet[\n"+
                this.user.toString()+"\n"+
                this.item.toString()+"\n"+
                "keys='"+this.keyword +"'\n"+
                "lang='" + this.lang + "'\n"+
                "]";
    }
}
