import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.xml.bind.DatatypeConverter;

import com.datastax.driver.core.*;


public class AcornYoutube {
	public static void main(String[] args) throws Exception {
		try {
			Conf.ParseArgs(args);

			DC.Init();

			YoutubeData.Load();

			System.exit(0);

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
			TestTopicFilter();
			TestReadMakingAttrsPopular();
			TestFetchOnDemand();

			// Cassandra cluster and session are not closed. Force quit.
			System.exit(0);
		} catch (Exception e) {
			System.err.println("Exception: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void CreateSchema() throws Exception {
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

	private static void XDcTrafficOnRead() throws Exception {
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
			Cass.ExecutionBarrier();

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

	private static void XDcTrafficOnReadCount() throws Exception {
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
			Cass.ExecutionBarrier();

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

	private static void _XDcTrafficOnRead(ConsistencyLevel cl, String obj_id) throws Exception {
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
		Cass.ExecutionBarrier();

		if (Cass.LocalDC().equals("us-east")) {
			Util.RxTx rxtx1 = Util.GetEth0RxTx();
			Cons.P("RX=%d TX=%d", rxtx1.rx - rxtx0.rx, rxtx1.tx - rxtx0.tx);
		}
	}

	private static void _XDcTrafficOnReadCount(ConsistencyLevel cl, String objId0, String objId1) throws Exception {
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
		Cass.ExecutionBarrier();

		if (Cass.LocalDC().equals("us-east")) {
			Util.RxTx rxtx1 = Util.GetEth0RxTx();
			Cons.P("RX=%d TX=%d", rxtx1.rx - rxtx0.rx, rxtx1.tx - rxtx0.tx);
		}
	}

	private static void TestPartialRep() throws Exception {
		try (Cons.MT _ = new Cons.MT("Testing partial replication ...")) {
			// Object id is constructed from the experiment id and _test_id.  You
			// cannot just use current date time, since the two machines on the east
			// and west won't see the same value.
			String objId0 = ObjIDFactory.Gen();
			String user_john = String.format("john-%s", Conf.ExpID());
			String user_jack = String.format("jack-%s", Conf.ExpID());
			String topic_tennis = String.format("tennis-%s", Conf.ExpID());
			String topic_uga = String.format("uga-%s", Conf.ExpID());
			String topic_dirty_sock = String.format("dirtysock-%s", Conf.ExpID());

			// Insert a record
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Inserting a record, %s ...", objId0)) {
					Cass.InsertRecordPartial(objId0, user_john, new TreeSet<String>(Arrays.asList(topic_tennis, topic_uga)));
				}
			}
			Cass.ExecutionBarrier();

			// Check the topic is not replicated to west.
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Expect to see the record immediately here ...")) {
					List<Row> rows = Cass.SelectRecordLocal(objId0);
					if (rows.size() != 1)
						throw new RuntimeException(String.format("Unexpected: objId0=%s rows.size()=%d", objId0, rows.size()));
				}
			} else if (Cass.LocalDC().equals("us-west")) {
				try (Cons.MT _1 = new Cons.MT("Checking to see the record is not replicated here ...")) {
					// Poll for a bit longer than the popularity broadcast interval to
					// make sure the record is not propagated.
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
						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
							System.out.printf(" no record found\n");
							break;
						}
					}
				}
			}
			Cass.ExecutionBarrier();

			// Make the topic tennis popular in the West with a write request. Write
			// is easier here. A read would have done the same.
			String objId1 = ObjIDFactory.Gen();
			Set<String> topics = new TreeSet<String>(Arrays.asList(topic_tennis, topic_dirty_sock));
			try (Cons.MT _1 = new Cons.MT("Making topics %s popular by inserting a record %s in the west ..."
						, String.join(", ", topics), objId1))
			{
				if (Cass.LocalDC().equals("us-west"))
					Cass.InsertRecordPartial(objId1, user_jack, topics);
				Cass.ExecutionBarrier();
			}

			long waitTime = Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500;
			try (Cons.MT _1 = new Cons.MT("Wait for a bit longer than the attribute popularity broadcast interval %s ms for the popularity change to propagate ...", waitTime)) {
				Thread.sleep(waitTime);
			}

			// Insert another record from the east. Expect it immediately visible in
			// the east and eventually visible in the west.
			String objId2 = ObjIDFactory.Gen();
			try (Cons.MT _1 = new Cons.MT("Inserting a record %s in the east ...", objId2)) {
				if (Cass.LocalDC().equals("us-east"))
					Cass.InsertRecordPartial(objId2, user_john, new TreeSet<String>(Arrays.asList(topic_tennis, topic_uga)));
				Cass.ExecutionBarrier();
			}
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Expecting to see the record immediately here ...")) {
					List<Row> rows = Cass.SelectRecordLocal(objId2);
					if (rows.size() != 1)
						throw new RuntimeException(String.format("Unexpected: objId2=%s rows.size()=%d", objId2, rows.size()));
				}
			} else {
				try (Cons.MT _1 = new Cons.MT("Checking to see the record replicated here ...")) {
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

						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
							System.out.printf("\n");
							throw new RuntimeException("Time out :(");
						}
					}
				}
			}
			Cass.ExecutionBarrier();

			// After (popularity monitor sliding window length + popularity broadcast
			// interval) time, insert another record in the east with the same topic,
			// which is not expected to be replicated to the west.
			try (Cons.MT _1 = new Cons.MT("Wait until popularity items expire ...")) {
				Thread.sleep(Conf.acornOptions.attr_pop_monitor_window_size_in_ms
						+ Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500);
			}
			String objId3 = ObjIDFactory.Gen();
			try (Cons.MT _1 = new Cons.MT("Inserting a record %s in the east ...", objId3)) {
				if (Cass.LocalDC().equals("us-east"))
					Cass.InsertRecordPartial(objId3, user_john, new TreeSet<String>(Arrays.asList(topic_tennis, topic_uga)));
				Cass.ExecutionBarrier();
			}
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Expecting to see the record immediately here ...")) {
					List<Row> rows = Cass.SelectRecordLocal(objId3);
					if (rows.size() != 1)
						throw new RuntimeException(String.format("Unexpected: objId3=%s rows.size()=%d", objId3, rows.size()));
				}
			} else {
				try (Cons.MT _1 = new Cons.MT("Checking to see the record not replicated here ...")) {
					// Poll for a bit longer than the popularity broadcast interval to
					// make sure the record is not propagated.
					Cons.Pnnl("Checking: ");
					long bt = System.currentTimeMillis();
					while (true) {
						List<Row> rows = Cass.SelectRecordLocal(objId3);
						if (rows.size() == 0) {
							System.out.printf(".");
							System.out.flush();
							Thread.sleep(100);
						} else {
							throw new RuntimeException(String.format("Unexpected: objId3=%s rows.size()=%d", objId3, rows.size()));
						}
						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
							System.out.printf(" no record found\n");
							break;
						}
					}
				}
			}
			Cass.ExecutionBarrier();
		}
	}

	private static void TestTopicFilter() throws Exception {
		try (Cons.MT _ = new Cons.MT("Testing topic filter ...")) {
			// Make tennis and throwbackthursday popular in the west.  Check if an
			// object with tennis is propagated, but an object with tbt is not.

			String user_jack = String.format("jack-%s", Conf.ExpID());
			String user_john = String.format("john-%s", Conf.ExpID());
			String user_vasek = String.format("vasek-%s", Conf.ExpID());
			String topic_tennis = String.format("tennis-%s", Conf.ExpID());
			String topic_tbt = "throwbackthursday";

			String objId0 = ObjIDFactory.Gen();
			Set<String> topics = new TreeSet<String>(Arrays.asList(topic_tennis, topic_tbt));
			try (Cons.MT _1 = new Cons.MT("Making topics %s popular by inserting a record %s in the west ..."
						, String.join(", ", topics), objId0))
			{
				if (Cass.LocalDC().equals("us-west"))
					Cass.InsertRecordPartial(objId0, user_jack, topics);
				Cass.ExecutionBarrier();
			}
			long waitTime = Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500;
			try (Cons.MT _1 = new Cons.MT("Wait for a bit longer than the attribute popularity broadcast interval %s ms for the popularity change to propagate ...", waitTime)) {
				Thread.sleep(waitTime);
			}

			// Insert a record with topic tennis from the east. Expect it immediately
			// visible in the east and eventually visible in the west.
			String objId1 = ObjIDFactory.Gen();
			try (Cons.MT _1 = new Cons.MT("Inserting a record %s in the east ...", objId1)) {
				if (Cass.LocalDC().equals("us-east"))
					Cass.InsertRecordPartial(objId1, user_john, new TreeSet<String>(Arrays.asList(topic_tennis)));
				Cass.ExecutionBarrier();
			}
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Expecting to see the record immediately here ...")) {
					List<Row> rows = Cass.SelectRecordLocal(objId1);
					if (rows.size() != 1)
						throw new RuntimeException(String.format("Unexpected: objId1=%s rows.size()=%d", objId1, rows.size()));
				}
			} else {
				try (Cons.MT _1 = new Cons.MT("Checking to see the record replicated here ...")) {
					Cons.Pnnl("Checking: ");
					long bt = System.currentTimeMillis();
					boolean first = true;
					while (true) {
						List<Row> rows = Cass.SelectRecordLocal(objId1);
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
							throw new RuntimeException(String.format("Unexpected: objId1=%s rows.size()=%d", objId1, rows.size()));

						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
							System.out.printf("\n");
							throw new RuntimeException("Time out :(");
						}
					}
				}
			}
			Cass.ExecutionBarrier();

			// Insert another with topic tbt from the east. Expect it immediately
			// visible in the east and not visible in the west.
			String objId2 = ObjIDFactory.Gen();
			try (Cons.MT _1 = new Cons.MT("Inserting a record %s in the east ...", objId2)) {
				if (Cass.LocalDC().equals("us-east"))
					Cass.InsertRecordPartial(objId2, user_vasek, new TreeSet<String>(Arrays.asList(topic_tbt)));
				Cass.ExecutionBarrier();
			}
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Expecting to see the record immediately here ...")) {
					List<Row> rows = Cass.SelectRecordLocal(objId2);
					if (rows.size() != 1)
						throw new RuntimeException(String.format("Unexpected: objId2=%s rows.size()=%d", objId2, rows.size()));
				}
			} else {
				try (Cons.MT _1 = new Cons.MT("Checking to see the record not replicated here ...")) {
					// Poll for a bit longer than the popularity broadcast interval to
					// make sure the record is not propagated.
					Cons.Pnnl("Checking: ");
					long bt = System.currentTimeMillis();
					while (true) {
						List<Row> rows = Cass.SelectRecordLocal(objId2);
						if (rows.size() == 0) {
							System.out.printf(".");
							System.out.flush();
							Thread.sleep(100);
						} else {
							throw new RuntimeException(String.format("Unexpected: objId2=%s rows.size()=%d", objId2, rows.size()));
						}
						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
							System.out.printf(" no record found\n");
							break;
						}
					}
				}
			}
			Cass.ExecutionBarrier();
		}
	}

	private static void TestReadMakingAttrsPopular() throws Exception {
		try (Cons.MT _ = new Cons.MT("Testing read request making attrs popular ...")) {
			String user_john = String.format("john-%s", Conf.ExpID());
			String user_jack = String.format("jack-%s", Conf.ExpID());
			String topic_tennis = String.format("tennis-%s", Conf.ExpID());
			String topic_uga = String.format("uga-%s", Conf.ExpID());
			String topic_dirty_sock = String.format("dirtysock-%s", Conf.ExpID());

			// Make the topic tennis popular in the west by inserting a record
			String objId0 = ObjIDFactory.Gen();
			if (Cass.LocalDC().equals("us-west")) {
				try (Cons.MT _1 = new Cons.MT("Inserting a record %s ...", objId0)) {
					Cass.InsertRecordPartial(objId0, user_jack, new TreeSet<String>(Arrays.asList(topic_tennis)));
				}
			}
			Cass.ExecutionBarrier();

			// Wait until the topics become unpopular in the west
			try (Cons.MT _1 = new Cons.MT("Wait until popularity items expire ...")) {
				Thread.sleep(Conf.acornOptions.attr_pop_monitor_window_size_in_ms
						+ Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500);
			}

			// Insert a record in the east
			String objId1 = ObjIDFactory.Gen();
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Inserting a record %s ...", objId1)) {
					Cass.InsertRecordPartial(objId1, user_john, new TreeSet<String>(Arrays.asList(topic_tennis, topic_uga)));
				}
			}
			// Expect the record immediately visible in the east and not visible in the west.
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Expect to see the record immediately in the east ...")) {
					List<Row> rows = Cass.SelectRecordLocal(objId1);
					if (rows.size() != 1)
						throw new RuntimeException(String.format("Unexpected: objId1=%s rows.size()=%d", objId1, rows.size()));
				}
			} else if (Cass.LocalDC().equals("us-west")) {
				try (Cons.MT _1 = new Cons.MT("Checking to see the record is not replicated in the west ...")) {
					// Poll for a bit longer than the popularity broadcast interval to
					// make sure the record is not propagated.
					Cons.Pnnl("Checking: ");
					long bt = System.currentTimeMillis();
					while (true) {
						List<Row> rows = Cass.SelectRecordLocal(objId1);
						if (rows.size() == 0) {
							System.out.printf(".");
							System.out.flush();
							Thread.sleep(100);
						} else {
							throw new RuntimeException(String.format("Unexpected: objId1=%s rows.size()=%d", objId1, rows.size()));
						}
						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
							System.out.printf(" no record found\n");
							break;
						}
					}
				}
			}
			Cass.ExecutionBarrier();

			// Read the fist record in the west to make the topics popular
			try (Cons.MT _1 = new Cons.MT("Make the topics popular in the west by reading the record ...")) {
				if (Cass.LocalDC().equals("us-west")) {
					List<Row> rows = Cass.SelectRecordLocal(objId0);
					if (rows.size() != 1)
						throw new RuntimeException(String.format("Unexpected: objId0=%s rows.size()=%d", objId0, rows.size()));
				}
				// Wait until the topic popularities propagate
				try (Cons.MT _2 = new Cons.MT("Wait a bit for the popularity to propagate ...")) {
					Thread.sleep(Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500);
				}
				Cass.ExecutionBarrier();
			}

			// Insert another record in the east
			String objId2 = ObjIDFactory.Gen();
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Inserting a record %s ...", objId2)) {
					Cass.InsertRecordPartial(objId2, user_john, new TreeSet<String>(Arrays.asList(topic_tennis, topic_uga)));
				}
			}
			// Expect the record immediately visible in the east and eventually visible in the west.
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Expect to see the record immediately in the east ...")) {
					List<Row> rows = Cass.SelectRecordLocal(objId2);
					if (rows.size() != 1)
						throw new RuntimeException(String.format("Unexpected: objId2=%s rows.size()=%d", objId2, rows.size()));
				}
			} else if (Cass.LocalDC().equals("us-west")) {
				try (Cons.MT _1 = new Cons.MT("Checking to see the record replicated in the west ...")) {
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

						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
							System.out.printf("\n");
							throw new RuntimeException("Time out :(");
						}
					}
				}
			}
			Cass.ExecutionBarrier();
		}
	}

	private static void TestFetchOnDemand() throws Exception {
		try (Cons.MT _ = new Cons.MT("Testing fetch on demand ...")) {
			String user_john_1 = String.format("john-1-%s", Conf.ExpID());
			String topic_tennis_1 = String.format("tennis-1-%s", Conf.ExpID());
			String topic_uga_1 = String.format("uga-1-%s", Conf.ExpID());

			Cass.ExecutionBarrier();

			// Insert a record in the east. Not replicated to the west.
			String objId0 = ObjIDFactory.Gen();
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Inserting a record %s in the east ...", objId0)) {
					Cass.InsertRecordPartial(objId0, user_john_1, new TreeSet<String>(Arrays.asList(topic_tennis_1, topic_uga_1)));
				}
			}
			// Expect the record immediately visible in the east and not visible in the west.
			if (Cass.LocalDC().equals("us-east")) {
				try (Cons.MT _1 = new Cons.MT("Expect to see the record immediately in the east ...")) {
					List<Row> rows = Cass.SelectRecordLocal(objId0);
					if (rows.size() != 1)
						throw new RuntimeException(String.format("Unexpected: objId0=%s rows.size()=%d", objId0, rows.size()));
				}
			} else if (Cass.LocalDC().equals("us-west")) {
				try (Cons.MT _1 = new Cons.MT("Fetch on demand in the west ...")) {
					// Poll for a bit longer than the popularity broadcast interval to
					// make sure the record is not propagated.
					Cons.Pnnl("Checking:");
					long bt = System.currentTimeMillis();
					while (true) {
						List<Row> rows = Cass.SelectRecordLocal(objId0);
						if (rows.size() != 0)
							throw new RuntimeException(String.format("Unexpected: rows.size()=%d", rows.size()));

						// Get a DC where the object is
						String dc = Cass.GetObjLoc(objId0);
						if (dc == null) {
							System.out.printf(" loc");
							System.out.flush();
							// Thread.sleep(100);
						} else {
							List<Row> rows1 = Cass.SelectRecordRemote(dc, objId0);
							if (rows1.size() != 1)
								throw new RuntimeException(String.format("Unexpected: rows1.size()=%d", rows1.size()));
							Row r = rows1.get(0);
							String objId = r.getString("obj_id");
							String user = r.getString("user");
							Set<String> topics = r.getSet("topics", String.class);
							//Cons.P("user={%s}", user);
							//Cons.P("topics={%s}", String.join(", ", topics));
							System.out.printf(" rf");
							System.out.flush();

							Cass.InsertRecordPartial(objId, user, topics);
							System.out.printf(" lw\n");
							break;
						}

						// Wait for a bit. Doesn't have to be broadcast interval. Okay for now.
						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
							System.out.printf("\n");
							throw new RuntimeException("Time out :(");
						}
					}
				}
			}
			Cass.ExecutionBarrier();
		}
	}
}
