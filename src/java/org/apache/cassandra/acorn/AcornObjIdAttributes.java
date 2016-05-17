package org.apache.cassandra.acorn;

import java.util.List;


public class AcornObjIdAttributes {
    public String objId;

    public String user;
    public List<String> topics;

    public AcornObjIdAttributes(
            String objId,
            String user,
            List<String> topics)
    {
        this.objId = objId;
        this.user = user;
        this.topics = topics;
    }

    public boolean IsAttrEmpty() {
        return ((user == null) && (topics == null || topics.size() == 0));
    }

    @Override
    public String toString() {
        return String.format("objId=%s user=%s topics=[%s]", objId, user, String.join(", ", topics));
    }
}
