package org.apache.cassandra.acorn;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.ArrayList;
import java.util.Queue;

import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AcornObjLoc implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(AcornObjLoc.class);

    // Queue objId that the location information will be inserted
    private static BlockingQueue<String> reqQ = new LinkedBlockingQueue<String>();
    private static Thread _thread = null;

    static {
        // Let a dedicated thread do the work for all requests.  Then, you only
        // need to synchronize on the request queue.
        _thread = new Thread(new AcornObjLoc());
        _thread.setName("AcornObjLoc");
        _thread.start();
    }

    private static String acornKsPrefix;
    private static String localDataCenter;

    public void run() {
        try {
            while (true) {
                // Fetch a request
                String objId = reqQ.take();
                _AddObjLoc(objId);
            }
        } catch (Exception e) {
            logger.warn("Acorn: Exception {}", e);
        }
    }

    public static void Add(AcornObjIdAttributes aoa, String acornKsPrefix_, String localDataCenter_) {
        try {
            // The parameters acornKsPrefix and localDataCenter are supposed to
            // be the same every time.
            if (acornKsPrefix == null)
                acornKsPrefix = acornKsPrefix_;
            if (localDataCenter == null) {
                localDataCenter = localDataCenter_;
            }

            // Enqueue the request
            reqQ.put(aoa.objId);
        } catch (InterruptedException e) {
            logger.warn("Acorn: Exception {}", e);
        }
    }

    private static void _AddObjLoc(String objId) {
        String q = String.format("UPDATE %s_obj_loc.obj_loc SET locations = locations + {'%s'} WHERE obj_id = '%s'"
                , acornKsPrefix, localDataCenter, objId);
        // Here the datacenter names contain '-'.
        //
        // https://docs.datastax.com/en/cql/3.1/cql/cql_using/use_set_t.html
        //
        // UPDATE acorn_obj_loc.obj_loc SET locations = locations + {'new_dc'} WHERE obj_id = 'aaa'
        //
        // It also create a row when it doesn't exist. Thank god.
        logger.warn("Acorn: q={}", q);

        QueryState state = QueryState.forInternalCalls();
        // Use CL LOCAL_ONE. It will eventually be propagated.
        QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE, new ArrayList<ByteBuffer>());

        QueryHandler qh = ClientState.getCQLQueryHandler();
        if (! qh.getClass().equals(QueryProcessor.class))
            throw new RuntimeException(String.format("Unexpected: qh.getClass()=%s", qh.getClass().getName()));
        QueryProcessor qp = (QueryProcessor) qh;
        final boolean acorn = true;
        qp.process(acorn, q, state, options);
    }
}
