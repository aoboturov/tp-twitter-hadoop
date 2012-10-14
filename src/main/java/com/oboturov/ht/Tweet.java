package com.oboturov.ht;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author aoboturov
 */
public class Tweet implements Writable {

    private User user;
    private Long time;
    private String post;

    public Tweet(final String user, final Long time, final String post) {
        this.user = new User();
        this.user.setName(user);
        this.time = time;
        this.post = post;
    }

    public User getUser() {
        return user;
    }

    public Long getTime() {
        return time;
    }

    public String getPost() {
        return post;
    }

    public void write(final DataOutput dataOutput) throws IOException {
        user.write(dataOutput);
        dataOutput.writeLong(time);
        dataOutput.writeUTF(post);
    }

    public void readFields(final DataInput dataInput) throws IOException {
        this.user.readFields(dataInput);
        this.time = dataInput.readLong();
        this.post = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tweet tweet = (Tweet) o;

        if (!post.equals(tweet.post)) return false;
        if (!time.equals(tweet.time)) return false;
        if (!user.equals(tweet.user)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = user.hashCode();
        result = 31 * result + time.hashCode();
        result = 31 * result + post.hashCode();
        return result;
    }
}
