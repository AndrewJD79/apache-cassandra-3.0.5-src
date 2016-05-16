package org.apache.cassandra.acorn;

import java.util.List;


public class AcornAttributes {
    public String user;
    public List<String> topics;

    public AcornAttributes(
            String user,
            List<String> topics)
    {
        this.user = user;
        this.topics = topics;
    }

    @Override
    public String toString() {
        return String.format("user=%s topics=[%s]", user, String.join(", ", topics));
    }
}
