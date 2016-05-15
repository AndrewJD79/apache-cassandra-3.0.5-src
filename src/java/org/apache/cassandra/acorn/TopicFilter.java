package org.apache.cassandra.acorn;

import java.util.Set;
import java.util.TreeSet;


public class TopicFilter {
	private static Set<String> topics;

    static {
        topics = new TreeSet<String>();

        // TODO: Load topic filters.

        //const string& fn = Conf::fn_topic_filter;
        //cout << boost::format("Loading topic filter from file %s ...\n") % fn;

        //ifstream ifs(fn);
        //if (! ifs.is_open())
        //    throw runtime_error(str(boost::format("Unable to open file %s") % fn));

        //auto sep = boost::is_any_of(" ");

        //string line;
        //while (getline(ifs, line)) {
        //    if (line.length() == 0)
        //        continue;
        //    if (line[0] == '#')
        //        continue;
        //    vector<string> t;
        //    boost::split(t, line, sep);
        //    if (t.size() != 2)
        //        throw runtime_error(str(boost::format("Unexpected format [%s]") % line));
        //    topics.insert(t[0]);
        //}

        //cout << "  loaded " << topics.size() << " topics\n";
    }

    public static boolean IsBlackListed(String t) {
        return topics.contains(t);
    }
}
