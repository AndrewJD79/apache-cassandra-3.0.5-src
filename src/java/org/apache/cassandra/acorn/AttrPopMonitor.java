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

import org.apache.cassandra.config.DatabaseDescriptor;
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
    private Map<String, Integer> popCntUser = new TreeMap<String, Integer>();
    private Map<String, Integer> popCntTopic = new TreeMap<String, Integer>();

    // Popular attribute items in previous broadcast epoch
    private Set<String> popUsersPrev = null;
    private Set<String> popTopicsPrev = null;

    // In milliseconds
    static final long popMonSlidingWindowSize = DatabaseDescriptor.getAcornOptions().attr_pop_monitor_window_size_in_ms;
    static final long popBcInterval = DatabaseDescriptor.getAcornOptions().attr_pop_broadcast_interval_in_ms;

    class SlidingWindowItem<T> {
        T attrItem;
        long expirationTime;

        SlidingWindowItem(T attrItem, long reqTime) {
            this.attrItem = attrItem;
            expirationTime = reqTime + popMonSlidingWindowSize;
        }
    }
    Queue<SlidingWindowItem> slidingWindowUser = new LinkedList<SlidingWindowItem>();
    Queue<SlidingWindowItem> slidingWindowTopic = new LinkedList<SlidingWindowItem>();

    public void run() {
        try {
            while (true) {
                // Fetch a request
                Req r = reqQ.poll(popBcInterval, TimeUnit.MILLISECONDS);
                long reqTime;
                if (r != null) {
                    // Note: May want to monitor user or topic popularity based on the
                    // configuration. Monitor both for now.
                    slidingWindowUser.add(new SlidingWindowItem(r.ut.user, r.reqTime));
                    popCntUser.put(r.ut.user, popCntUser.getOrDefault(r.ut.user, 0) + 1);
                    //logger.warn("Acorn: popular user + {}", r.ut.user);

                    for (String t: r.ut.topics) {
                        if (AttrFilter.IsTopicBlackListed(t))
                            continue;
                        slidingWindowTopic.add(new SlidingWindowItem(t, r.reqTime));
                        popCntTopic.put(t, popCntTopic.getOrDefault(t, 0) + 1);
                        //logger.warn("Acorn: popular topic + {}", t);
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
            SlidingWindowItem<String> swi = slidingWindowUser.peek();
            if (swi == null)
                break;
            //logger.warn("Acorn: swi.expirationTime={} reqTime={}", swi.expirationTime, reqTime);
            if (swi.expirationTime > reqTime)
                break;
            String attrItem = swi.attrItem;
            slidingWindowUser.remove();
            int cnt = popCntUser.get(attrItem) - 1;
            if (cnt > 0) {
                popCntUser.put(attrItem, cnt);
            } else {
                popCntUser.remove(attrItem);
            }
            //logger.warn("Acorn: popular user - {}", attrItem);
        }

        while (true) {
            SlidingWindowItem<String> swi = slidingWindowTopic.peek();
            if (swi == null)
                break;
            //logger.warn("Acorn: swi.expirationTime={} reqTime={}", swi.expirationTime, reqTime);
            if (swi.expirationTime > reqTime)
                break;
            String attrItem = swi.attrItem;
            slidingWindowTopic.remove();
            int cnt = popCntTopic.get(attrItem) - 1;
            if (cnt > 0) {
                popCntTopic.put(attrItem, cnt);
            } else {
                popCntTopic.remove(attrItem);
            }
            //logger.warn("Acorn: popular topic - {}", attrItem);
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

        long curBcEpoch = (reqTime - firstBcReqTime) / popBcInterval;
        //logger.warn("Acorn: prevBcEpoch={} curBcEpoch={}", prevBcEpoch, curBcEpoch);
        if (curBcEpoch == prevBcEpoch)
            return;

        // Build popUsersCur and popTopicsCur. Attribute items will be added in
        // the natural (sorted) order
        Set<String> popUsersCur = new TreeSet<String>();
        for (Map.Entry<String, Integer> e : popCntUser.entrySet()) {
            String user = e.getKey();
            int cnt = e.getValue();
            // Note: The sensitivity analysis of popularity threshould can be done here.
            if (cnt > 0)
                popUsersCur.add(user);
        }
        Set<String> popTopicsCur = new TreeSet<String>();
        for (Map.Entry<String, Integer> e : popCntTopic.entrySet()) {
            String t = e.getKey();
            int cnt = e.getValue();
            // Note: The sensitivity analysis of popularity threshould can be done here.
            if (cnt > 0)
                popTopicsCur.add(t);
        }
        //logger.warn("Acorn: popUsersCur={} popTopicsCur={}", String.join(",", popUsersCur) , String.join(",", popTopicsCur));

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

        // This early return doesn't change prevBcEpoch and makes the next
        // broadcast in popularity change more responsive without paying any
        // extra broadcast cost. Good.
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
            q.append(String.format(" DELETE FROM %s_attr_pop.%s_topic where topic = '%s';"
                        , acornKsPrefix, localDataCenterCql, t));
        q.append(" APPLY BATCH");
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
