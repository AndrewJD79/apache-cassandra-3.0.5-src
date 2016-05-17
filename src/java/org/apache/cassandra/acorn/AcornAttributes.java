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

    public boolean Empty() {
        return ((user == null) && (topics == null || topics.size() == 0));
    }

    @Override
    public String toString() {
        return String.format("user=%s topics=[%s]", user, String.join(", ", topics));
    }
}
