import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import javax.xml.bind.DatatypeConverter;

import com.datastax.driver.core.*;


public class AcornYoutube {
	public static void main(String[] args) throws Exception {
		try {
			Conf.ParseArgs(args);

			DC.Init();

			// Overlap Cass.Init() and YouTube.Load() to save time. Cons.P()s are
			// messed up, but not a big deal.
			Thread tCassInit = new Thread() {
				public void run() {
					try {
						Cass.Init();
					} catch (Exception e) {
						System.out.printf("Exception: %s\n%s\n", e, Util.GetStackTrace(e));
						System.exit(1);
					}
				}
			};
			tCassInit.start();

			Thread tYoutubeDataLoad = new Thread() {
				public void run() {
					try {
						// This needs to be after DC.Init(), but can be overlapped with Cass.Init().
						YoutubeData.Load();
					} catch (Exception e) {
						System.out.printf("Exception: %s\n%s\n", e, Util.GetStackTrace(e));
						System.exit(1);
					}
				}
			};
			tYoutubeDataLoad.start();

			tCassInit.join();
			tYoutubeDataLoad.join();

			CreateSchema();

			RunFullReplication();

			// Seems like leftover Cassandra cluster and session objects prevent the
			// process from terminating. Force quit. Don't think it's a big deal for
			// this experiment.
			System.exit(0);
		} catch (Exception e) {
			System.out.printf("Exception: %s\n%s\n", e, Util.GetStackTrace(e));
			System.exit(1);
		}
	}

	private static void CreateSchema() throws Exception {
		// Note: Assume us-east is always there as a leader.
		if (Cass.LocalDC().equals("us-east")) {
			if (Cass.SchemaExist()) {
				Cons.P("Schema already exists.");
			} else {
				Cass.CreateSchema();
			}
		}

		Cass.WaitForSchemaCreation();
	}

	private static void RunFullReplication() throws Exception {
		_AgreeOnStartTime();

		int numThreads = 100;
		Cons.P("Making requests with %d threads ...", numThreads);

		// TODO ProgMon.Start();

		// Start all requester threads
		List<Thread> reqThreads = new ArrayList<Thread>();
		for (int i = 0; i < numThreads; i ++) {
			Thread t = new Thread(new ReqThread());
			t.start();
			reqThreads.add(t);
		}

		for (Thread t: reqThreads)
			t.join();

		// TODO ProgMon.Stop();
	}

	private static class ReqThread implements Runnable {
		public void run() {
			try {
				while (true) {
					YoutubeData.Req r = YoutubeData.allReqs.poll(0, TimeUnit.NANOSECONDS);
					if (r == null)
						break;
					//Cons.P(String.format("%s tid=%d", r, Thread.currentThread().getId()));

					if (r.type == YoutubeData.Req.Type.W) {
						SimTime.SleepUntilSimulatedTime(r);
						// TODO dbCli.DbWriteMeasureTime(r);
					} else {
						SimTime.SleepUntilSimulatedTime(r);
						// TODO dbCli.DbReadMeasureTime(r);
					}
				}
			} catch (Exception e) {
				// Better stop the process all together here.
				//   com.datastax.driver.core.exceptions.NoHostAvailableException is an example.
				System.out.printf("Exception: %s\n%s\n", e, Util.GetStackTrace(e));
				System.exit(1);
			}
		}
	}

	/////////////////////

	// TODO
	//protected void DbReadMeasureTime(Op op) throws InterruptedException {
	//	long begin = System.nanoTime();
	//	DbRead(op);
	//	long end = System.nanoTime();
	//	LatMon.Read(end - begin);
	//}

	//protected void DbWriteMeasureTime(Op op) throws InterruptedException {
	//	long begin = System.nanoTime();
	//	DbWrite(op);
	//	long end = System.nanoTime();
	//	LatMon.Write(end - begin);
	//}

	//protected void DbWrite(Op op) throws InterruptedException {
	//	// Simulate a write
	//	Thread.sleep(10);
	//}

	//protected void DbRead(Op op) throws InterruptedException {
	//	// Simulate a read, which is slower than write
	//	Thread.sleep(20);
	//}


	/////////////////////////////////////////

	private static void _AgreeOnStartTime() throws Exception {
		// Agree on the future, start time.
		// - Issue an execution barrier and measure the time from the east.
		// - East post a reasonable future time and everyone polls the value.
		//   - If the value is in a reasonable future, like at least 100 ms in the
		//     future, then go.
		//   - Otherwise, throw an exception.
		try (Cons.MT _ = new Cons.MT("Agreeing on the start time ...")) {
			// This, the first one, could take long. Depending on the last operation.
			Cass.ExecutionBarrier();
			// From the second one, it osilates with 2 nodes. With more than 2,
			// it won't be as big.
			long maxLapTime = Math.max(Cass.ExecutionBarrier(), Cass.ExecutionBarrier());
			Cons.P("maxLapTime=%d ms", maxLapTime);

			// System.currentTimeMillis() is the time from 1970 in UTC. Good!
			long startTime;
			if (Cass.LocalDC().equals("us-east")) {
				long now = System.currentTimeMillis();
				startTime = now + maxLapTime * 5;
				Cass.WriteStartTime(startTime);
			} else {
				startTime = Cass.ReadStartTimeUntilSucceed();
			}
			SimTime.SetStartTime(startTime);
		}
	}

//	static class ObjIDFactory {
//		private static int _test_id = 0;
//		static String Gen() {
//			return String.format("%s-%03d", Conf.ExpID(), _test_id ++);
//		}
//	}
//
//	private static void TestPartialRep() throws Exception {
//		try (Cons.MT _ = new Cons.MT("Testing partial replication ...")) {
//			// Object id is constructed from the experiment id and _test_id.  You
//			// cannot just use current date time, since the two machines on the east
//			// and west won't see the same value.
//			String objId0 = ObjIDFactory.Gen();
//			String user_john = String.format("john-%s", Conf.ExpID());
//			String user_jack = String.format("jack-%s", Conf.ExpID());
//			String topic_tennis = String.format("tennis-%s", Conf.ExpID());
//			String topic_uga = String.format("uga-%s", Conf.ExpID());
//			String topic_dirty_sock = String.format("dirtysock-%s", Conf.ExpID());
//
//			// Insert a record
//			if (Cass.LocalDC().equals("us-east")) {
//				try (Cons.MT _1 = new Cons.MT("Inserting a record, %s ...", objId0)) {
//					Cass.InsertRecordPartial(objId0, user_john, new TreeSet<String>(Arrays.asList(topic_tennis, topic_uga)));
//				}
//			}
//			Cass.ExecutionBarrier();
//
//			// Check the topic is not replicated to west.
//			if (Cass.LocalDC().equals("us-east")) {
//				try (Cons.MT _1 = new Cons.MT("Expect to see the record immediately here ...")) {
//					List<Row> rows = Cass.SelectRecordLocal(objId0);
//					if (rows.size() != 1)
//						throw new RuntimeException(String.format("Unexpected: objId0=%s rows.size()=%d", objId0, rows.size()));
//				}
//			} else if (Cass.LocalDC().equals("us-west")) {
//				try (Cons.MT _1 = new Cons.MT("Checking to see the record is not replicated here ...")) {
//					// Poll for a bit longer than the popularity broadcast interval to
//					// make sure the record is not propagated.
//					Cons.Pnnl("Checking: ");
//					long bt = System.currentTimeMillis();
//					while (true) {
//						List<Row> rows = Cass.SelectRecordLocal(objId0);
//						if (rows.size() == 0) {
//							System.out.printf(".");
//							System.out.flush();
//							Thread.sleep(100);
//						} else {
//							throw new RuntimeException(String.format("Unexpected: objId0=%s rows.size()=%d", objId0, rows.size()));
//						}
//						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
//							System.out.printf(" no record found\n");
//							break;
//						}
//					}
//				}
//			}
//			Cass.ExecutionBarrier();
//
//			// Make the topic tennis popular in the West with a write request. Write
//			// is easier here. A read would have done the same.
//			String objId1 = ObjIDFactory.Gen();
//			Set<String> topics = new TreeSet<String>(Arrays.asList(topic_tennis, topic_dirty_sock));
//			try (Cons.MT _1 = new Cons.MT("Making topics %s popular by inserting a record %s in the west ..."
//						, String.join(", ", topics), objId1))
//			{
//				if (Cass.LocalDC().equals("us-west"))
//					Cass.InsertRecordPartial(objId1, user_jack, topics);
//				Cass.ExecutionBarrier();
//			}
//
//			long waitTime = Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500;
//			try (Cons.MT _1 = new Cons.MT("Wait for a bit longer than the attribute popularity broadcast interval %s ms for the popularity change to propagate ...", waitTime)) {
//				Thread.sleep(waitTime);
//			}
//
//			// Insert another record from the east. Expect it immediately visible in
//			// the east and eventually visible in the west.
//			String objId2 = ObjIDFactory.Gen();
//			try (Cons.MT _1 = new Cons.MT("Inserting a record %s in the east ...", objId2)) {
//				if (Cass.LocalDC().equals("us-east"))
//					Cass.InsertRecordPartial(objId2, user_john, new TreeSet<String>(Arrays.asList(topic_tennis, topic_uga)));
//				Cass.ExecutionBarrier();
//			}
//			if (Cass.LocalDC().equals("us-east")) {
//				try (Cons.MT _1 = new Cons.MT("Expecting to see the record immediately here ...")) {
//					List<Row> rows = Cass.SelectRecordLocal(objId2);
//					if (rows.size() != 1)
//						throw new RuntimeException(String.format("Unexpected: objId2=%s rows.size()=%d", objId2, rows.size()));
//				}
//			} else {
//				try (Cons.MT _1 = new Cons.MT("Checking to see the record replicated here ...")) {
//					Cons.Pnnl("Checking: ");
//					long bt = System.currentTimeMillis();
//					boolean first = true;
//					while (true) {
//						List<Row> rows = Cass.SelectRecordLocal(objId2);
//						if (rows.size() == 0) {
//							if (first) {
//								System.out.printf(" ");
//								first = false;
//							}
//							System.out.printf(".");
//							System.out.flush();
//							Thread.sleep(100);
//						} else if (rows.size() == 1) {
//							System.out.printf(" got it!\n");
//							break;
//						} else
//							throw new RuntimeException(String.format("Unexpected: objId2=%s rows.size()=%d", objId2, rows.size()));
//
//						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
//							System.out.printf("\n");
//							throw new RuntimeException("Time out :(");
//						}
//					}
//				}
//			}
//			Cass.ExecutionBarrier();
//
//			// After (popularity monitor sliding window length + popularity broadcast
//			// interval) time, insert another record in the east with the same topic,
//			// which is not expected to be replicated to the west.
//			try (Cons.MT _1 = new Cons.MT("Wait until popularity items expire ...")) {
//				Thread.sleep(Conf.acornOptions.attr_pop_monitor_window_size_in_ms
//						+ Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500);
//			}
//			String objId3 = ObjIDFactory.Gen();
//			try (Cons.MT _1 = new Cons.MT("Inserting a record %s in the east ...", objId3)) {
//				if (Cass.LocalDC().equals("us-east"))
//					Cass.InsertRecordPartial(objId3, user_john, new TreeSet<String>(Arrays.asList(topic_tennis, topic_uga)));
//				Cass.ExecutionBarrier();
//			}
//			if (Cass.LocalDC().equals("us-east")) {
//				try (Cons.MT _1 = new Cons.MT("Expecting to see the record immediately here ...")) {
//					List<Row> rows = Cass.SelectRecordLocal(objId3);
//					if (rows.size() != 1)
//						throw new RuntimeException(String.format("Unexpected: objId3=%s rows.size()=%d", objId3, rows.size()));
//				}
//			} else {
//				try (Cons.MT _1 = new Cons.MT("Checking to see the record not replicated here ...")) {
//					// Poll for a bit longer than the popularity broadcast interval to
//					// make sure the record is not propagated.
//					Cons.Pnnl("Checking: ");
//					long bt = System.currentTimeMillis();
//					while (true) {
//						List<Row> rows = Cass.SelectRecordLocal(objId3);
//						if (rows.size() == 0) {
//							System.out.printf(".");
//							System.out.flush();
//							Thread.sleep(100);
//						} else {
//							throw new RuntimeException(String.format("Unexpected: objId3=%s rows.size()=%d", objId3, rows.size()));
//						}
//						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
//							System.out.printf(" no record found\n");
//							break;
//						}
//					}
//				}
//			}
//			Cass.ExecutionBarrier();
//		}
//	}
//
//	private static void TestTopicFilter() throws Exception {
//		try (Cons.MT _ = new Cons.MT("Testing topic filter ...")) {
//			// Make tennis and throwbackthursday popular in the west.  Check if an
//			// object with tennis is propagated, but an object with tbt is not.
//
//			String user_jack = String.format("jack-%s", Conf.ExpID());
//			String user_john = String.format("john-%s", Conf.ExpID());
//			String user_vasek = String.format("vasek-%s", Conf.ExpID());
//			String topic_tennis = String.format("tennis-%s", Conf.ExpID());
//			String topic_tbt = "throwbackthursday";
//
//			String objId0 = ObjIDFactory.Gen();
//			Set<String> topics = new TreeSet<String>(Arrays.asList(topic_tennis, topic_tbt));
//			try (Cons.MT _1 = new Cons.MT("Making topics %s popular by inserting a record %s in the west ..."
//						, String.join(", ", topics), objId0))
//			{
//				if (Cass.LocalDC().equals("us-west"))
//					Cass.InsertRecordPartial(objId0, user_jack, topics);
//				Cass.ExecutionBarrier();
//			}
//			long waitTime = Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500;
//			try (Cons.MT _1 = new Cons.MT("Wait for a bit longer than the attribute popularity broadcast interval %s ms for the popularity change to propagate ...", waitTime)) {
//				Thread.sleep(waitTime);
//			}
//
//			// Insert a record with topic tennis from the east. Expect it immediately
//			// visible in the east and eventually visible in the west.
//			String objId1 = ObjIDFactory.Gen();
//			try (Cons.MT _1 = new Cons.MT("Inserting a record %s in the east ...", objId1)) {
//				if (Cass.LocalDC().equals("us-east"))
//					Cass.InsertRecordPartial(objId1, user_john, new TreeSet<String>(Arrays.asList(topic_tennis)));
//				Cass.ExecutionBarrier();
//			}
//			if (Cass.LocalDC().equals("us-east")) {
//				try (Cons.MT _1 = new Cons.MT("Expecting to see the record immediately here ...")) {
//					List<Row> rows = Cass.SelectRecordLocal(objId1);
//					if (rows.size() != 1)
//						throw new RuntimeException(String.format("Unexpected: objId1=%s rows.size()=%d", objId1, rows.size()));
//				}
//			} else {
//				try (Cons.MT _1 = new Cons.MT("Checking to see the record replicated here ...")) {
//					Cons.Pnnl("Checking: ");
//					long bt = System.currentTimeMillis();
//					boolean first = true;
//					while (true) {
//						List<Row> rows = Cass.SelectRecordLocal(objId1);
//						if (rows.size() == 0) {
//							if (first) {
//								System.out.printf(" ");
//								first = false;
//							}
//							System.out.printf(".");
//							System.out.flush();
//							Thread.sleep(100);
//						} else if (rows.size() == 1) {
//							System.out.printf(" got it!\n");
//							break;
//						} else
//							throw new RuntimeException(String.format("Unexpected: objId1=%s rows.size()=%d", objId1, rows.size()));
//
//						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
//							System.out.printf("\n");
//							throw new RuntimeException("Time out :(");
//						}
//					}
//				}
//			}
//			Cass.ExecutionBarrier();
//
//			// Insert another with topic tbt from the east. Expect it immediately
//			// visible in the east and not visible in the west.
//			String objId2 = ObjIDFactory.Gen();
//			try (Cons.MT _1 = new Cons.MT("Inserting a record %s in the east ...", objId2)) {
//				if (Cass.LocalDC().equals("us-east"))
//					Cass.InsertRecordPartial(objId2, user_vasek, new TreeSet<String>(Arrays.asList(topic_tbt)));
//				Cass.ExecutionBarrier();
//			}
//			if (Cass.LocalDC().equals("us-east")) {
//				try (Cons.MT _1 = new Cons.MT("Expecting to see the record immediately here ...")) {
//					List<Row> rows = Cass.SelectRecordLocal(objId2);
//					if (rows.size() != 1)
//						throw new RuntimeException(String.format("Unexpected: objId2=%s rows.size()=%d", objId2, rows.size()));
//				}
//			} else {
//				try (Cons.MT _1 = new Cons.MT("Checking to see the record not replicated here ...")) {
//					// Poll for a bit longer than the popularity broadcast interval to
//					// make sure the record is not propagated.
//					Cons.Pnnl("Checking: ");
//					long bt = System.currentTimeMillis();
//					while (true) {
//						List<Row> rows = Cass.SelectRecordLocal(objId2);
//						if (rows.size() == 0) {
//							System.out.printf(".");
//							System.out.flush();
//							Thread.sleep(100);
//						} else {
//							throw new RuntimeException(String.format("Unexpected: objId2=%s rows.size()=%d", objId2, rows.size()));
//						}
//						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
//							System.out.printf(" no record found\n");
//							break;
//						}
//					}
//				}
//			}
//			Cass.ExecutionBarrier();
//		}
//	}
//
//	private static void TestReadMakingAttrsPopular() throws Exception {
//		try (Cons.MT _ = new Cons.MT("Testing read request making attrs popular ...")) {
//			String user_john = String.format("john-%s", Conf.ExpID());
//			String user_jack = String.format("jack-%s", Conf.ExpID());
//			String topic_tennis = String.format("tennis-%s", Conf.ExpID());
//			String topic_uga = String.format("uga-%s", Conf.ExpID());
//			String topic_dirty_sock = String.format("dirtysock-%s", Conf.ExpID());
//
//			// Make the topic tennis popular in the west by inserting a record
//			String objId0 = ObjIDFactory.Gen();
//			if (Cass.LocalDC().equals("us-west")) {
//				try (Cons.MT _1 = new Cons.MT("Inserting a record %s ...", objId0)) {
//					Cass.InsertRecordPartial(objId0, user_jack, new TreeSet<String>(Arrays.asList(topic_tennis)));
//				}
//			}
//			Cass.ExecutionBarrier();
//
//			// Wait until the topics become unpopular in the west
//			try (Cons.MT _1 = new Cons.MT("Wait until popularity items expire ...")) {
//				Thread.sleep(Conf.acornOptions.attr_pop_monitor_window_size_in_ms
//						+ Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500);
//			}
//
//			// Insert a record in the east
//			String objId1 = ObjIDFactory.Gen();
//			if (Cass.LocalDC().equals("us-east")) {
//				try (Cons.MT _1 = new Cons.MT("Inserting a record %s ...", objId1)) {
//					Cass.InsertRecordPartial(objId1, user_john, new TreeSet<String>(Arrays.asList(topic_tennis, topic_uga)));
//				}
//			}
//			// Expect the record immediately visible in the east and not visible in the west.
//			if (Cass.LocalDC().equals("us-east")) {
//				try (Cons.MT _1 = new Cons.MT("Expect to see the record immediately in the east ...")) {
//					List<Row> rows = Cass.SelectRecordLocal(objId1);
//					if (rows.size() != 1)
//						throw new RuntimeException(String.format("Unexpected: objId1=%s rows.size()=%d", objId1, rows.size()));
//				}
//			} else if (Cass.LocalDC().equals("us-west")) {
//				try (Cons.MT _1 = new Cons.MT("Checking to see the record is not replicated in the west ...")) {
//					// Poll for a bit longer than the popularity broadcast interval to
//					// make sure the record is not propagated.
//					Cons.Pnnl("Checking: ");
//					long bt = System.currentTimeMillis();
//					while (true) {
//						List<Row> rows = Cass.SelectRecordLocal(objId1);
//						if (rows.size() == 0) {
//							System.out.printf(".");
//							System.out.flush();
//							Thread.sleep(100);
//						} else {
//							throw new RuntimeException(String.format("Unexpected: objId1=%s rows.size()=%d", objId1, rows.size()));
//						}
//						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
//							System.out.printf(" no record found\n");
//							break;
//						}
//					}
//				}
//			}
//			Cass.ExecutionBarrier();
//
//			// Read the fist record in the west to make the topics popular
//			try (Cons.MT _1 = new Cons.MT("Make the topics popular in the west by reading the record ...")) {
//				if (Cass.LocalDC().equals("us-west")) {
//					List<Row> rows = Cass.SelectRecordLocal(objId0);
//					if (rows.size() != 1)
//						throw new RuntimeException(String.format("Unexpected: objId0=%s rows.size()=%d", objId0, rows.size()));
//				}
//				// Wait until the topic popularities propagate
//				try (Cons.MT _2 = new Cons.MT("Wait a bit for the popularity to propagate ...")) {
//					Thread.sleep(Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500);
//				}
//				Cass.ExecutionBarrier();
//			}
//
//			// Insert another record in the east
//			String objId2 = ObjIDFactory.Gen();
//			if (Cass.LocalDC().equals("us-east")) {
//				try (Cons.MT _1 = new Cons.MT("Inserting a record %s ...", objId2)) {
//					Cass.InsertRecordPartial(objId2, user_john, new TreeSet<String>(Arrays.asList(topic_tennis, topic_uga)));
//				}
//			}
//			// Expect the record immediately visible in the east and eventually visible in the west.
//			if (Cass.LocalDC().equals("us-east")) {
//				try (Cons.MT _1 = new Cons.MT("Expect to see the record immediately in the east ...")) {
//					List<Row> rows = Cass.SelectRecordLocal(objId2);
//					if (rows.size() != 1)
//						throw new RuntimeException(String.format("Unexpected: objId2=%s rows.size()=%d", objId2, rows.size()));
//				}
//			} else if (Cass.LocalDC().equals("us-west")) {
//				try (Cons.MT _1 = new Cons.MT("Checking to see the record replicated in the west ...")) {
//					Cons.Pnnl("Checking: ");
//					long bt = System.currentTimeMillis();
//					boolean first = true;
//					while (true) {
//						List<Row> rows = Cass.SelectRecordLocal(objId2);
//						if (rows.size() == 0) {
//							if (first) {
//								System.out.printf(" ");
//								first = false;
//							}
//							System.out.printf(".");
//							System.out.flush();
//							Thread.sleep(100);
//						} else if (rows.size() == 1) {
//							System.out.printf(" got it!\n");
//							break;
//						} else
//							throw new RuntimeException(String.format("Unexpected: objId2=%s rows.size()=%d", objId2, rows.size()));
//
//						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
//							System.out.printf("\n");
//							throw new RuntimeException("Time out :(");
//						}
//					}
//				}
//			}
//			Cass.ExecutionBarrier();
//		}
//	}
//
//	private static void TestFetchOnDemand() throws Exception {
//		try (Cons.MT _ = new Cons.MT("Testing fetch on demand ...")) {
//			String user_john_1 = String.format("john-1-%s", Conf.ExpID());
//			String topic_tennis_1 = String.format("tennis-1-%s", Conf.ExpID());
//			String topic_uga_1 = String.format("uga-1-%s", Conf.ExpID());
//
//			Cass.ExecutionBarrier();
//
//			// Insert a record in the east. Not replicated to the west.
//			String objId0 = ObjIDFactory.Gen();
//			if (Cass.LocalDC().equals("us-east")) {
//				try (Cons.MT _1 = new Cons.MT("Inserting a record %s in the east ...", objId0)) {
//					Cass.InsertRecordPartial(objId0, user_john_1, new TreeSet<String>(Arrays.asList(topic_tennis_1, topic_uga_1)));
//				}
//			}
//			// Expect the record immediately visible in the east and not visible in the west.
//			if (Cass.LocalDC().equals("us-east")) {
//				try (Cons.MT _1 = new Cons.MT("Expect to see the record immediately in the east ...")) {
//					List<Row> rows = Cass.SelectRecordLocal(objId0);
//					if (rows.size() != 1)
//						throw new RuntimeException(String.format("Unexpected: objId0=%s rows.size()=%d", objId0, rows.size()));
//				}
//			} else if (Cass.LocalDC().equals("us-west")) {
//				try (Cons.MT _1 = new Cons.MT("Fetch on demand in the west ...")) {
//					// Poll for a bit longer than the popularity broadcast interval to
//					// make sure the record is not propagated.
//					Cons.Pnnl("Checking:");
//					long bt = System.currentTimeMillis();
//					while (true) {
//						List<Row> rows = Cass.SelectRecordLocal(objId0);
//						if (rows.size() != 0)
//							throw new RuntimeException(String.format("Unexpected: rows.size()=%d", rows.size()));
//
//						// Get a DC where the object is
//						String dc = Cass.GetObjLoc(objId0);
//						if (dc == null) {
//							System.out.printf(" loc");
//							System.out.flush();
//							// Thread.sleep(100);
//						} else {
//							List<Row> rows1 = Cass.SelectRecordRemote(dc, objId0);
//							if (rows1.size() != 1)
//								throw new RuntimeException(String.format("Unexpected: rows1.size()=%d", rows1.size()));
//							Row r = rows1.get(0);
//							String objId = r.getString("obj_id");
//							String user = r.getString("user");
//							Set<String> topics = r.getSet("topics", String.class);
//							//Cons.P("user={%s}", user);
//							//Cons.P("topics={%s}", String.join(", ", topics));
//							System.out.printf(" rf");
//							System.out.flush();
//
//							Cass.InsertRecordPartial(objId, user, topics);
//							System.out.printf(" lw\n");
//							break;
//						}
//
//						// Wait for a bit. Doesn't have to be broadcast interval. Okay for now.
//						if (System.currentTimeMillis() - bt > Conf.acornOptions.attr_pop_broadcast_interval_in_ms + 500) {
//							System.out.printf("\n");
//							throw new RuntimeException("Time out :(");
//						}
//					}
//				}
//			}
//			Cass.ExecutionBarrier();
//		}
//	}
}
