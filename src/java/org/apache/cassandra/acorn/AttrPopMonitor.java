package org.apache.cassandra.acorn;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.TreeMap;

import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AttrPopMonitor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(AttrPopMonitor.class);

    private static BlockingQueue<Req> reqQ = new LinkedBlockingQueue<Req>();
    private static Thread _thread = null;

    static {
        // Let a dedicated thread do the work for all requests.  Then, you only
        // need to synchronize on the request queue.
        _thread = new Thread(new AttrPopMonitor());
        _thread.start();
    }

    private static String acornKsPrefix;
    private static String localDataCenter;
    private static String localDataCenterCql;

	// Popularity count per attribute item
	private Map<String, Integer> pcUser = new TreeMap<String, Integer>();
	private Map<String, Integer> pcTopic = new TreeMap<String, Integer>();

    // Popular attribute items in previous broadcast epoch
    private Set<String> popTopicsPrev = null;
	private Set<String> popUsersPrev = null;

    // TODO: make it configurable from cassandra.yaml
    // In milliseconds
    static final long popMonSlidingWindowSize = 2000;

    class SlidingWindowItem<T> {
        T attrItem;
        long expirationTime;

        SlidingWindowItem(T attrItem, long reqTime) {
            this.attrItem = attrItem;
            expirationTime = reqTime + popMonSlidingWindowSize;
        }
    }
    Queue<SlidingWindowItem> swUser;
    Queue<SlidingWindowItem> swTopic;

    AttrPopMonitor() {
        swUser = new LinkedList<SlidingWindowItem>();
        swTopic = new LinkedList<SlidingWindowItem>();
    }

    public void run() {
        try {
            while (true) {
                // Fetch a request
                // TODO: make it configurable
                Req r = reqQ.poll(2000, TimeUnit.MILLISECONDS);
                long reqTime;
                if (r != null) {
                    // Note: May want to monitor user or topic popularity based on the
                    // configuration. Monitor both for now.
                    swUser.add(new SlidingWindowItem(r.ut.user, r.reqTime));
                    pcUser.put(r.ut.user, pcUser.getOrDefault(r.ut.user, 0) + 1);
                    logger.warn("Acorn: popular user + {}", r.ut.user);

                    for (String t: r.ut.topics) {
                        if (TopicFilter.IsBlackListed(t))
                            continue;
                        swTopic.add(new SlidingWindowItem(t, r.reqTime));
                        pcTopic.put(t, pcTopic.getOrDefault(t, 0) + 1);
                        logger.warn("Acorn: popular topic + {}", t);
                    }

                    reqTime = r.reqTime;
                } else {
                    reqTime = System.currentTimeMillis();
                }

                _ExpirePopularities(reqTime);
                _PropagateToRemoteDCs(reqTime);
            }
        } catch (Exception e) {
            logger.warn("Acorn: Exception {}", e);
        }
    }

    static class Req {
        long reqTime;
        Mutation.UserTopics ut;

        Req(Mutation.UserTopics ut) {
            reqTime = System.currentTimeMillis();
            this.ut = ut;
        }
    }

    // Note: Play with popularity detection threshould. Hope I have some time
    // for this.
    public static void SetPopular(Mutation.UserTopics ut, String acornKsPrefix_, String localDataCenter_) {
        try {
            // The parameters acornKsPrefix and localDataCenter are supposed to
            // be the same every time.
            if (acornKsPrefix == null)
                acornKsPrefix = acornKsPrefix_;
            if (localDataCenter == null) {
                localDataCenter = localDataCenter_;
                localDataCenterCql = localDataCenter.replace("-", "_");
            }

            // Enqueue the request
            reqQ.put(new Req(ut));
        } catch (InterruptedException e) {
            logger.warn("Acorn: Exception {}", e);
        }
    }

    private void _ExpirePopularities(long reqTime) {
        while (true) {
            SlidingWindowItem<String> swi = swUser.peek();
            if (swi == null)
                break;
            logger.warn("Acorn: swi.expirationTime={} reqTime={}", swi.expirationTime, reqTime);
            if (swi.expirationTime > reqTime)
                break;
            String attrItem = swi.attrItem;
            swUser.remove();
            pcUser.put(attrItem, pcUser.get(attrItem) - 1);
            logger.warn("Acorn: popular user - {}", attrItem);
        }

        while (true) {
            SlidingWindowItem<String> swi = swTopic.peek();
            if (swi == null)
                break;
            logger.warn("Acorn: swi.expirationTime={} reqTime={}", swi.expirationTime, reqTime);
            if (swi.expirationTime > reqTime)
                break;
            String attrItem = swi.attrItem;
            swTopic.remove();
            pcTopic.put(attrItem, pcTopic.get(attrItem) - 1);
            logger.warn("Acorn: popular topic - {}", attrItem);
        }
    }

    private long firstBcReqTime;
    private long prevBcEpoch = -1;

    private void _PropagateToRemoteDCs(long reqTime) {
        if (prevBcEpoch == -1) {
            firstBcReqTime = reqTime;
            prevBcEpoch = 0;
            return;
        }

        // TODO: make it configurable
        long curBcEpoch = (reqTime - firstBcReqTime) / 2000;
        if (curBcEpoch == prevBcEpoch)
            return;

        // Build popUsersCur and popTopicsCur. Attribute items will be added in
        // the natural (sorted) order
        Set<String> popUsersCur = new TreeSet<String>();
        for (Map.Entry<String, Integer> e : pcUser.entrySet()) {
            String user = e.getKey();
            int cnt = e.getValue();
            // Note: The sensitivity analysis of popularity threshould can be done here.
            if (cnt > 0)
                popUsersCur.add(user);
        }
        Set<String> popTopicsCur = new TreeSet<String>();
        for (Map.Entry<String, Integer> e : pcTopic.entrySet()) {
            String t = e.getKey();
            int cnt = e.getValue();
            // Note: The sensitivity analysis of popularity threshould can be done here.
            if (cnt > 0)
                popTopicsCur.add(t);
        }
        if (popUsersCur.size() == 0 && popTopicsCur.size() == 0) {
            // This early return and the one below don't change prevBcEpoch and
            // makes the next broadcast in popularity change more responsive
            // without paying any extra broadcast cost. Good.
            return;
        }
        logger.warn("Acorn: popUsersCur={} popTopicsCur={}"
                , String.join(",", popUsersCur)
                , String.join(",", popTopicsCur));

        // Calc diffs: what attributes to add and delete.
        Set<String> popUsersAdded;
        Set<String> popUsersDeleted;
        if (popUsersPrev == null) {
            popUsersAdded = popUsersCur;
            popUsersDeleted = new TreeSet<String>();
        } else {
            popUsersAdded = new TreeSet<String>();
            popUsersDeleted = new TreeSet<String>();
            for (String e: popUsersCur)
                if (! popUsersPrev.contains(e))
                    popUsersAdded.add(e);
            for (String e: popUsersPrev)
                if (! popUsersCur.contains(e))
                    popUsersDeleted.add(e);
        }

        Set<String> popTopicsAdded;
        Set<String> popTopicsDeleted;
        if (popTopicsPrev == null) {
            popTopicsAdded = popTopicsCur;
            popTopicsDeleted = new TreeSet<String>();
        } else {
            popTopicsAdded = new TreeSet<String>();
            popTopicsDeleted = new TreeSet<String>();
            for (String e: popTopicsCur)
                if (! popTopicsPrev.contains(e))
                    popTopicsAdded.add(e);
            for (String e: popTopicsPrev)
                if (! popTopicsCur.contains(e))
                    popTopicsDeleted.add(e);
        }

        if (popUsersAdded.size() == 0 && popUsersDeleted.size() == 0
                && popTopicsAdded.size() == 0 && popTopicsDeleted.size() == 0)
            return;

        StringBuilder q = new StringBuilder();
        q.append("BEGIN BATCH");
        for (String u: popUsersAdded)
            q.append(String.format(" INSERT INTO %s_attr_pop.%s_user (user_id) VALUES ('%s');"
                        , acornKsPrefix, localDataCenterCql, u));
        for (String u: popUsersDeleted)
            q.append(String.format(" DELETE FROM %s_attr_pop.%s_user where user_id = '%s';"
                        , acornKsPrefix, localDataCenterCql, u));
        for (String t: popTopicsAdded)
            q.append(String.format(" INSERT INTO %s_attr_pop.%s_topic (topic) VALUES ('%s');"
                        , acornKsPrefix, localDataCenterCql, t));
        for (String t: popTopicsDeleted)
            q.append(String.format(" DELETE FROM %s_attr_pop.%s_user where user_id = '%s';"
                        , acornKsPrefix, localDataCenterCql, t));
        q.append(" APPLY BATCH");
        // TODO: make more test cases and verify this
        logger.warn("Acorn: q={}", q.toString());

        QueryState state = QueryState.forInternalCalls();
        // Use CL LOCAL_ONE. It will eventually be propagated.
        QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE, new ArrayList<ByteBuffer>());

        QueryHandler qh = ClientState.getCQLQueryHandler();
        if (! qh.getClass().equals(QueryProcessor.class))
            throw new RuntimeException(String.format("Unexpected: qh.getClass()=%s", qh.getClass().getName()));
        QueryProcessor qp = (QueryProcessor) qh;
        final boolean acorn = true;
        qp.process(acorn, q.toString(), state, options);

        popUsersPrev = popUsersCur;
        popTopicsPrev = popTopicsCur;
        prevBcEpoch = curBcEpoch;
    }
}
