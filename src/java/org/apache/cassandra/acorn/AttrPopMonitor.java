package org.apache.cassandra.acorn;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

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
        // TODO: Let the dedicated thread do the work for all requests.
        // TODO: Then you only need to synchronize on the request queue.
        // TODO: A lot of the variables can be made non-static.
        _thread = new Thread(new AttrPopMonitor());
        _thread.start();
    }

    private static String acornKsPrefix;
    private static String localDataCenter;

	// Attr popularity in the local DC.
    private Set<String> popTopicsCur;
	private Set<String> popTopicsPrev = null;

    // Note: When you do the parameter sensitivity analysis of the popularity
    // detection threshould, the data structures of these and the PopExpireQ
    // need to be redesigned.
	private Set<String> popUsersCur;
	private Set<String> popUsersPrev = null;

    // TODO:
	//private PopExpireQ<String> peqTopics;
	//private PopExpireQ<String> peqUsers;

	private int prevBcEpoch = -1;

    AttrPopMonitor() {
        popTopicsCur = new TreeSet<String>();
        popUsersCur = new TreeSet<String>();
    }

    public void run() {
        try {
            while (true) {
                // Wait and take a request
                Req r = reqQ.take();

                // Note: May want to monitor user or topic popularity based on the
                // configuration. Monitor both for now.
                //
                // TODO:
                //peqUsers.Push(r.reqTime, r.ut.user);
                popUsersCur.add(r.ut.user);

                for (String t: r.ut.topics) {
                    if (TopicFilter.IsBlackListed(t))
                        continue;

                    // TODO:
                    //peqTopics.Push(r.reqTime, t);
                    popTopicsCur.add(t);
                }

                //_ExpirePopularities(r.reqTime);
                //_PropagateToRemoteDCs(r.reqTime);
                _PropagateToRemoteDCs();
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
    // to do this.
    public static void SetPopular(Mutation.UserTopics ut, String acornKsPrefix_, String localDataCenter_) {
        try {
            // acornKsPrefix and localDataCenter are believed to be the same.
            if (acornKsPrefix == null)
                acornKsPrefix = acornKsPrefix_;
            if (localDataCenter == null)
                localDataCenter = localDataCenter_;

            // Enqueue the request
            reqQ.put(new Req(ut));
        } catch (InterruptedException e) {
            logger.warn("Acorn: Exception {}", e);
        }
    }

// Drop attributes that are expired
//    private void _ExpirePopularities(long reqTime) {
//        while (true) {
//            PopExpireQ.Entry e = peqUsers.Head();
//            if (e == null)
//                break;
//            if (reqTime <= e.expTime)
//                break;
//
//            String id = e.id;
//            if (popUsersCur.remove(id) == false)
//                throw new RuntimeException(String.format("Unable to remove(find) id %s from popUsersCur", id));
//            peqUsers.Pop();
//        }
//
//        while (true) {
//            PopExpireQ.Entry e = peqTopics.Head();
//            if (e == null)
//                break;
//            if (reqTime <= e.expTime)
//                break;
//
//            String id = e.id;
//            if (popTopicsCur.remove(id) == false)
//                throw new RuntimeException(String.format("Unable to remove(find) id %s from popTopicsCur", id));
//            peqTopics.Pop();
//        }
//    }

    //private void _PropagateToRemoteDCs(long reqTime) {
    private void _PropagateToRemoteDCs() {
        // TODO: Testing to see if it works

        if (popUsersCur.size() == 0 && popTopicsCur.size() == 0)
            return;

        StringBuilder q = new StringBuilder();
        q.append("BEGIN BATCH");
        for (String u: popUsersCur)
            q.append(String.format(" INSERT INTO %s_attr_pop.%s_user (user_id) VALUES ('%s');"
                        , acornKsPrefix, localDataCenter.replace("-", "_"), u));
        for (String t: popTopicsCur)
            q.append(String.format(" INSERT INTO %s_attr_pop.%s_topic (topic) VALUES ('%s');"
                        , acornKsPrefix, localDataCenter.replace("-", "_"), t));
        q.append("APPLY BATCH");
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
    }

//        // Propagate user popularity in the local DC to other DCs.  Sending only the
//        // difference from the previous epoch is what should be done in the real
//        // environment.
//
//        // TODO: need a start reference time. When a record is first inserted
//        // to the partial acorn space.
//        // TODO: rename to AcornMetadata.fistLocalInsertTime
//        // In milliseconds.
//        long dur = reqTime - YoutubeData::oldest_created_at;
//
//        // TODO: get from the configuration
//        long bcEpoch = dur / Conf::pop_bcint;
//        if (prevBcEpoch == bcEpoch)
//            return;
//
//        popUsersPrev = popUsersCur;
//        // TODO: Write popUsersPrev to the attr popularity keyspace
//        // TODO: reference MakeAttrPopularThread
//
//        popTopicsPrev = popTopicsCur;
//        // TODO: Write popTopicsPrev to the attr popularity keyspace
//
//        prevBcEpoch = bcEpoch;
//    }
}


// TODO: not sure if I will need this
//friend class Mons;

//// Popularity expiration queue. Not thread safe by design.
//class PopExpireQ<T> {
//    class Entry {
//        // it can be either obj id, user id, topic, or anything.
//        T id;
//        // Expiration time
//        long expTime;
//        // TODO: I wonder if I can use a standard Java container
//        // TODO: Wait... Java doesn't have pointers.
//        Entry* prev;
//        Entry* next;
//
//        Entry(T id, long curTime) {
//            this.id = id;
//            expTime = curTime + Conf::pop_ws;
//        }
//
//        void UpdateExpTime(long curTime) {
//            expTime = curTime + Conf::pop_ws;
//        }
//    }
//
//    // Queue implemented by linked list. ordered by expTime. _head is the
//    // oldest, _tail is the youngest. Items are inserted to _tail and popped
//    // from _head.
//    Entry* _head;
//    Entry* _tail;
//    //int _q_size = 0;
//    std::map<T, Entry*> _by_ids;
//
//    void _QInsertToTail(Entry e) {
//        if (_head == NULL) {
//            // insert to the empty list
//            _head = _tail = e;
//            e.prev = NULL;
//            e.next = NULL;
//        } else {
//            // insert to the tail
//            e.prev = _tail;
//            e.next = NULL;
//            _tail.next = e;
//            _tail = e;
//        }
//
//        //_q_size ++;
//    }
//
//    void _QRemove(Entry e) {
//        // e is not NULL
//
//        if (_head == e) {
//            _head = e.next;
//        } else {
//            Entry* p = e.prev;
//            if (p)
//                p.next = e.next;
//        }
//
//        if (_tail == e) {
//            _tail = e.prev;
//        } else {
//            Entry* n = e.next;
//            if (n)
//                n.prev = e.prev;
//        }
//
//        //_q_size --;
//    }
//
//    void _QRemoveDeleteHead() {
//        Entry* n = _head.next;
//        delete _head;
//        _head = n;
//
//        //_q_size --;
//    }
//
//public:
//    PopExpireQ()
//        : _head(NULL), _tail(NULL)
//    {}
//
//    ~PopExpireQ() {
//        // delete any expire popularity items left
//        while (_head)
//            _QRemoveDeleteHead();
//    }
//
//    void Push(long curTime, T& id) {
//        auto it = _by_ids.find(id);
//        if (it == _by_ids.end()) {
//            Entry* e = new Entry(id, curTime);
//            _by_ids[id] = e;
//            _QInsertToTail(e);
//        } else {
//            Entry e = it.second;
//            e.UpdateExpTime(curTime);
//            _QRemove(e);
//            _QInsertToTail(e);
//        }
//        // cout << _q_size << " " << flush;
//    }
//
//    Entry* Head() {
//        return _head;
//    }
//
//    void Pop() {
//        if (_head == NULL)
//            return;
//        if (_by_ids.erase(_head.id) != 1)
//            throw std::runtime_error(str(boost::format("Unable to erase(find) id %1%") % _head.id));
//        _QRemoveDeleteHead();
//    }
//}


//private static class MakeAttrPopularThread implements Runnable
//{
//    public void run() {
//        try {
//            StringBuilder q = new StringBuilder();
//            q.append("BEGIN BATCH");
//            for (String t: ut.topics) {
//                q.append(
//                        String.format(
//                            " INSERT INTO %s_attr_pop.%s_topic (topic) VALUES ('%s');"
//                            , acornKsPrefix, localDataCenter.replace("-", "_"), t));
//            }
//            q.append("APPLY BATCH");
//            logger.warn("Acorn: q={}", q.toString());
//
//            QueryState state = QueryState.forInternalCalls();
//            // Use CL LOCAL_ONE. It will eventually be propagated.
//            QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE, new ArrayList<ByteBuffer>());
//
//            QueryHandler qh = ClientState.getCQLQueryHandler();
//            if (! qh.getClass().equals(QueryProcessor.class))
//                throw new RuntimeException(String.format("Unexpected: qh.getClass()=%s", qh.getClass().getName()));
//            QueryProcessor qp = (QueryProcessor) qh;
//            final boolean acorn = true;
//            qp.process(acorn, q.toString(), state, options);
//        } catch (Exception e) {
//            logger.warn("Acorn: {}", e);
//        }
//    }
//}
