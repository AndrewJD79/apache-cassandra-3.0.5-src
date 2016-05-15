import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import javax.xml.bind.DatatypeConverter;

import com.datastax.driver.core.*;


public class PartialRep {

//	private static void TestReadAfterWrite()
//		throws java.lang.InterruptedException, UnknownHostException {
//		Cass.Sync(_tid);
//		System.out.printf("TestReadAfterWrite ");
//		System.out.flush();
//		int b_tid = _tid;
//		if (Conf.dc.equals("DC0")) {
//			while (true) {
//				Cass.Execute(String.format("INSERT INTO pr_ondemand_client.t0 "
//							+ "(exp_dt, tid, s_dc, pr_tdcs) "
//							+ "VALUES ('%s', %d, '%s', {'%s'});",
//							Conf.dt_begin, _tid, Conf.hns[0], Conf.hns[0]));
//				ResultSet rs = Cass.Execute(String.format("SELECT * FROM pr_ondemand_client.t0 "
//							+ "WHERE exp_dt='%s' AND tid=%d;",
//							Conf.dt_begin, _tid));
//				int size = rs.all().size();
//				if (_tid % 100 == 0) {
//					System.out.printf(".");
//					System.out.flush();
//				}
//				_tid ++;
//				if (size == 0)
//					break;
//			}
//			System.out.printf(" got 0 in %d pair(s) of INSERT and SELECT.\n", _tid - b_tid);
//		}
//	}
//
//	private static void _SelectLocalUntil(int tid, int exp_v) {
//		long bt = System.currentTimeMillis();
//		System.out.printf("  SelectLocalUntil");
//		System.out.flush();
//		while (true) {
//			int r = Cass.SelectLocal(tid);
//			System.out.printf(" %d", r);
//			System.out.flush();
//			if (r == exp_v)
//				break;
//			if (System.currentTimeMillis() - bt > 5000)
//				throw new RuntimeException("Timed out");
//		}
//		System.out.printf("\n");
//	}
//
//	private static void _SelectLocalUntil2(int tid, int exp_v)
//		throws java.lang.InterruptedException {
//		long bt = System.currentTimeMillis();
//		System.out.printf("  SelectLocalUntil");
//		System.out.flush();
//		while (true) {
//			int r = Cass.SelectLocal(tid);
//			System.out.printf(" %d", r);
//			System.out.flush();
//			if (r == exp_v)
//				break;
//			if (System.currentTimeMillis() - bt > 5000)
//				throw new RuntimeException("Timed out");
//			Thread.sleep(1000);
//		}
//		System.out.printf("\n");
//	}
//
//	private static void TestRegularInsertSelect()
//		throws java.lang.InterruptedException, UnknownHostException {
//		Cass.Sync(_tid);
//		System.out.printf("TestRegularInsertSelect\n");
//		if (Conf.dc.equals("DC0")) {
//			Cass.Insert(_tid);
//		}
//		_SelectLocalUntil(_tid, 1);
//
//		_tid ++;
//	}
//
//	private static void _SelectLocal(int tid, int exp_v) {
//		System.out.printf("  SelectLocal ");
//		System.out.flush();
//		int r = Cass.SelectLocal(_tid);
//		System.out.printf("%d\n", r);
//		if (r != exp_v)
//			throw new RuntimeException(String.format("got %d. expected %d.", r, exp_v));
//	}
//
//	private static void _SelectAsyncFetchOnDemand(int tid, int exp_v) {
//		System.out.printf("  SelectAsyncFetchOnDemand ");
//		System.out.flush();
//		long bt = System.currentTimeMillis();
//		int r = Cass.SelectFetchOnDemand(_tid, "mdc-s40", false);
//		long et = System.currentTimeMillis();
//		System.out.printf("%d %d ms\n", r, et - bt);
//		if (r != exp_v)
//			System.out.printf("    INTERESTING!!! got %d. expected %d.\n", r, exp_v);
//	}
//
//	private static void TestAsyncFetchOnDemand()
//		throws java.lang.InterruptedException, UnknownHostException {
//		Cass.Sync(_tid);
//		System.out.printf("TestAsyncFetchOnDemand\n");
//		if (Conf.dc.equals("DC0")) {
//			Cass.InsertToDC0(_tid);
//			_SelectLocalUntil(_tid, 1);
//		} else if (Conf.dc.equals("DC1")) {
//			_SelectLocal(_tid, 0);
//
//			// wait 1 sec to see if DC0 propagate the insert by mistake
//			Thread.sleep(1000);
//			_SelectLocal(_tid, 0);
//
//			long bt = System.currentTimeMillis();
//			// Rarely, it fetches new data from the src DC. I guess, when the two DCs
//			// are too close and the local DC is loaded. So getting 1 may not be an error.
//			_SelectAsyncFetchOnDemand(_tid, 0);
//			_SelectLocalUntil(_tid, 1);
//			long et = System.currentTimeMillis();
//			System.out.printf("    got new data in %d ms from PartialRep\n", et - bt);
//		} else
//			throw new RuntimeException("unknown dc: " + Conf.dc);
//		_tid ++;
//	}
//
//	private static void _SelectSyncFetchOnDemand(int tid, int exp_v) {
//		System.out.printf("  SelectSyncFetchOnDemand ");
//		System.out.flush();
//		long bt = System.currentTimeMillis();
//		int r = Cass.SelectFetchOnDemand(_tid, "mdc-s40", true);
//		long et = System.currentTimeMillis();
//		System.out.printf("%d %d ms\n", r, et - bt);
//		if (r != exp_v)
//			throw new RuntimeException(String.format("got %d. expected %d.", r, exp_v));
//	}
//
//	private static void TestSyncFetchOnDemand()
//		throws java.lang.InterruptedException, UnknownHostException {
//		Cass.Sync(_tid);
//		System.out.printf("TestSyncFetchOnDemand\n");
//		if (Conf.dc.equals("DC0")) {
//			Cass.InsertToDC0(_tid);
//			_SelectLocalUntil(_tid, 1);
//		} else if (Conf.dc.equals("DC1")) {
//			_SelectLocal(_tid, 0);
//
//			// wait 1 sec to see if DC0 propagate the insert by mistake
//			Thread.sleep(1000);
//			_SelectLocal(_tid, 0);
//
//			_SelectSyncFetchOnDemand(_tid, 1);
//			_SelectLocal(_tid, 1);
//		} else
//			throw new RuntimeException("unknown dc: " + Conf.dc);
//		_tid ++;
//	}

	public static void main(String[] args) throws Exception {
		try {
			Conf.ParseArgs(args);

			Cass.Init();

			if ((! Cass.LocalDC().equals("us-east"))
					&& (! Cass.LocalDC().equals("us-west")))
				throw new RuntimeException(String.format("Unexpected: local_dc=%s", Cass.LocalDC()));

			CreateSchema();

			// Check x-DC traffic while reading. CL LOCAL_ONE doesn't make inter-DC
			// traffic.
			//XDcTrafficOnRead();
			//XDcTrafficOnReadCount();

			TestPartialRep();

			// TODO TestFetchOnDemand();
			// It should probably be asynchronous

			// TODO: clean up
			//TestSyncFetchOnDemand();
			//TestAsyncFetchOnDemand();
			//TestRegularInsertSelect();

			// this takes a really long time. ?? TODO: what is it? Need to remind.
			//TestReadAfterWrite();

			Cass.Close();
		} catch (Exception e) {
			System.err.println("Exception: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void CreateSchema() throws InterruptedException, UnknownHostException {
		if (Cass.LocalDC().equals("us-east")) {
			if (Cass.SchemaExist()) {
				Cons.P("Schema already exists.");
			} else {
				Cass.CreateSchema();
			}
		}

		Cass.WaitForSchemaCreation();
	}

	static class ObjIDFactory {
		private static int _test_id = 0;
		static String Gen() {
			return String.format("%s-%03d", Conf.ExpID(), _test_id ++);
		}
	}

	private static void XDcTrafficOnRead() throws InterruptedException, IOException {
		try (Cons.MT _ = new Cons.MT("Monitor traffic while reading ...")) {
			// Insert a big record to the acorn keyspace and keep reading it while
			// monitoring the inter-DC traffic.
			int objSize = 10000;
			String obj_id = ObjIDFactory.Gen();
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Inserting a record obj_id=%s size=%d ...", obj_id, objSize)) {
					Cass.InsertRandomToRegular(obj_id, objSize);
				}
			}
			Cass.Sync();

			// Only ALL seems to have xDC communication.
			//   ONE         93 ms RX=  236 TX=  406
			//   LOCAL_ONE   74 ms RX=  567 TX=  517
			//   ALL       7816 ms RX=23482 TX=32326
			//
			// Another observation is that they don't seem to exchange the full
			// object. 1MB of traffic was expected, but was only like 2%.
			//
			// Total execution time is not important here. ONE and LOCAL_ONE don't
			// wait for inter-DC messages, if they ever existed.
			//
			// Query: select * from %s.t0 where c0='%s'
			_XDcTrafficOnRead(ConsistencyLevel.ONE,       obj_id);
			_XDcTrafficOnRead(ConsistencyLevel.LOCAL_ONE, obj_id);
			_XDcTrafficOnRead(ConsistencyLevel.ALL,       obj_id);

			// This means with LOCAL_ONE, read (select) is always DC-local. I though
			// I've seen something different. Oh well.
		}
	}

	private static void XDcTrafficOnReadCount() throws InterruptedException, IOException {
		try (Cons.MT _ = new Cons.MT("Monitor traffic while reading ...")) {
			// Insert a big record to the acorn keyspace and keep reading it while
			// monitoring the inter-DC traffic.
			int objSize = 10000;
			String objId0 = ObjIDFactory.Gen();
			String objId1 = ObjIDFactory.Gen();
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Inserting a record objId0=%s objId1=%s size=%d ...", objId0, objId1, objSize)) {
					Cass.InsertRandomToRegular(objId0, objSize);
					Cass.InsertRandomToRegular(objId1, objSize);
				}
			}
			Cass.Sync();

			// Again, only ALL seems to have xDC communication.
			//   ONE         71 ms RX= 2184 TX=12539
			//   LOCAL_ONE   57 ms RX= 3411 TX=12948
			//   ALL       7843 ms RX=24928 TX=43292
			//
			// Query: select count(*) from %s.t0 where c0 in ('%s', '%s')
			_XDcTrafficOnReadCount(ConsistencyLevel.ONE,       objId0, objId1);
			_XDcTrafficOnReadCount(ConsistencyLevel.LOCAL_ONE, objId0, objId1);
			_XDcTrafficOnReadCount(ConsistencyLevel.ALL,       objId0, objId1);
		}
	}

	private static void _XDcTrafficOnRead(ConsistencyLevel cl, String obj_id) throws InterruptedException, IOException {
		Util.RxTx rxtx0 = null;
		if (Cass.LocalDC().equals("us-east")) {
			rxtx0 = Util.GetEth0RxTx();

			int cnt = 100;
			try (Cons.MT _1 = new Cons.MT("Selecting the record %s %d times with CL %s ...", obj_id, cnt, cl)) {
				boolean first = true;
				for (int i = 0; i < cnt; i ++) {
					List<Row> rows = Cass.SelectFromRegular(cl, obj_id);

					for (Row r : rows) {
						String c0 = r.getString("c0");
						ByteBuffer c1 = r.getBytes("c1");
						// http://stackoverflow.com/questions/679298/gets-byte-array-from-a-bytebuffer-in-java
						byte[] b = new byte[c1.remaining()];
						c1.get(b);
						// http://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java
						//Cons.P("%s %s", c0, javax.xml.bind.DatatypeConverter.printHexBinary(b));
					}

					if (i % 20 != 19)
						continue;
					if (first) {
						Cons.Pnnl(".");
						first = false;
					} else {
						System.out.printf(".");
						System.out.flush();
					}
				}
				System.out.printf("\n");
			}
		}
		Cass.Sync();

		if (Cass.LocalDC().equals("us-east")) {
			Util.RxTx rxtx1 = Util.GetEth0RxTx();
			Cons.P("RX=%d TX=%d", rxtx1.rx - rxtx0.rx, rxtx1.tx - rxtx0.tx);
		}
	}

	private static void _XDcTrafficOnReadCount(ConsistencyLevel cl, String objId0, String objId1) throws InterruptedException, IOException {
		Util.RxTx rxtx0 = null;
		if (Cass.LocalDC().equals("us-east")) {
			rxtx0 = Util.GetEth0RxTx();

			int cnt = 50;
			try (Cons.MT _1 = new Cons.MT("Selecting (count) the records %s and %s %d times with CL %s ...", objId0, objId1, cnt, cl)) {
				boolean first = true;
				for (int i = 0; i < cnt; i ++) {
					Cass.SelectCountFromRegular(cl, objId0, objId1);

					if (i % 20 != 19)
						continue;
					if (first) {
						Cons.Pnnl(".");
						first = false;
					} else {
						System.out.printf(".");
						System.out.flush();
					}
				}
				System.out.printf("\n");
			}
		}
		Cass.Sync();

		if (Cass.LocalDC().equals("us-east")) {
			Util.RxTx rxtx1 = Util.GetEth0RxTx();
			Cons.P("RX=%d TX=%d", rxtx1.rx - rxtx0.rx, rxtx1.tx - rxtx0.tx);
		}
	}

	private static void TestPartialRep() throws InterruptedException {
		try (Cons.MT _ = new Cons.MT("Testing partial replication ...")) {
			// Insert a record
			//
			// Object id is constructed from the experiment id and _test_id.  You
			// cannot just use current date time, since the two machines on the east
			// and west won't have the same value.
			String objId0 = ObjIDFactory.Gen();
			String topic_tennis = String.format("tennis-%s", Conf.ExpID());
			String topic_uga = String.format("uga-%s", Conf.ExpID());
			String topic_dirty_sock = String.format("dirtysock-%s", Conf.ExpID());
			// Sync (like execution barrier). Wait till everyone gets here.
			Cass.Sync();

			// Insert a record
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Inserting a record, %s ...", objId0)) {
					Cass.InsertRecordPartial(objId0, "john", new TreeSet<String>(Arrays.asList(topic_tennis, topic_uga)));
				}
			}
			Cass.Sync();

			// Check the topic is not replicated to west.
			if (Cass.LocalDC().equals("us-east")) {
				List<Row> rows = Cass.SelectRecordLocal(objId0);
				if (rows.size() != 1)
					throw new RuntimeException(String.format("Unexpected: objId0=%s rows.size()=%d", objId0, rows.size()));
			} else if (Cass.LocalDC().equals("us-west")) {
				try (Cons.MT _1 = new Cons.MT("Checking to see no record is replicated here ...")) {
					// Poll for 2 secs making sure the record is not propagated.
					Cons.Pnnl("Checking: ");
					long bt = System.currentTimeMillis();
					while (true) {
						List<Row> rows = Cass.SelectRecordLocal(objId0);
						if (rows.size() == 0) {
							System.out.printf(".");
							System.out.flush();
							Thread.sleep(100);
						} else {
							throw new RuntimeException(String.format("Unexpected: objId0=%s rows.size()=%d", objId0, rows.size()));
						}
						if (System.currentTimeMillis() - bt > 2000) {
							System.out.printf(" no record found\n");
							break;
						}
					}
				}
			}
			Cass.Sync();

			// Make the topic tennis popular in the West with a write request. Write
			// is easier here. A read would have done the same.
			String objId1 = ObjIDFactory.Gen();
			if (Cass.LocalDC().equals("us-west"))
				Cass.InsertRecordPartial(objId1, "jack", new TreeSet<String>(Arrays.asList(topic_tennis, topic_dirty_sock)));
			Cass.Sync();

			// Insert another record from the east. Expect it immediately visible in
			// the east and eventually visible in the west.
			String objId2 = ObjIDFactory.Gen();
			if (Cass.LocalDC().equals("us-east"))
				Cass.InsertRecordPartial(objId2, "john", new TreeSet<String>(Arrays.asList(topic_tennis, topic_uga)));
			Cass.Sync();

			if (Cass.LocalDC().equals("us-east")) {
				List<Row> rows = Cass.SelectRecordLocal(objId2);
				if (rows.size() != 1)
					throw new RuntimeException(String.format("Unexpected: objId2=%s rows.size()=%d", objId2, rows.size()));
			} else {
				try (Cons.MT _1 = new Cons.MT("Checking to see a record replicated here ...")) {
					Cons.Pnnl("Checking: ");
					long bt = System.currentTimeMillis();
					boolean first = true;
					while (true) {
						List<Row> rows = Cass.SelectRecordLocal(objId2);
						if (rows.size() == 0) {
							if (first) {
								System.out.printf(" ");
								first = false;
							}
							System.out.printf(".");
							System.out.flush();
							Thread.sleep(100);
						} else if (rows.size() == 1) {
							System.out.printf(" got it!\n");
							break;
						} else
							throw new RuntimeException(String.format("Unexpected: objId2=%s rows.size()=%d", objId2, rows.size()));

						if (System.currentTimeMillis() - bt > 2000) {
							System.out.printf("\n");
							throw new RuntimeException("Time out :(");
						}
					}
				}
			}

			// TODO: After 2 seconds (set the popularity detection sliding window
			// length to 2 sec in simulation time), insert another record in the east
			// with the same topic, which is not expected to be replicated to the
			// west.
		}
	}

	private static void TestFetchOnDemand() {
	}
}
