package org.apache.cassandra.acorn;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URL;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;


public class AttrFilter {
    private static final Logger logger = LoggerFactory.getLogger(AttrFilter.class);

    //private static Set<String> users = new TreeSet<String>();
    private static Set<String> topics = new TreeSet<String>();

    static {
        try {
            ClassLoader loader = DatabaseDescriptor.class.getClassLoader();

            // Filtering by user id doesn't make sense. For now, only for
            // topics.  It may make sense for other attributes.
            {
                String fn = DatabaseDescriptor.getAcornOptions().attr_filter_topic;
                URL url = loader.getResource(fn);
                try (BufferedReader br = new BufferedReader(new FileReader(url.getFile()))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        //logger.warn("Acorn: {}", line);
                        // http://stackoverflow.com/questions/225337/how-do-i-split-a-string-with-any-whitespace-chars-as-delimiters
                        String[] t = line.split("\\s+");
                        if (t.length != 2)
                            throw new RuntimeException(String.format("Unexpected format: [%s]", line));
                        topics.add(t[0]);
                    }
                }
                logger.warn("Acorn: Loaded topic filter from {}. {} items.", fn, topics.size());
            }
        } catch (Exception e) {
            logger.warn("Acorn: Exception: {}", e);
        }
    }

    //public static boolean IsUsersBlackListed(String e) {
    //    return users.contains(e);
    //}

    public static boolean IsTopicBlackListed(String e) {
        return topics.contains(e);
    }
}
