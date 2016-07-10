import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
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
			Conf.Init(args);

			DC.Init();

			if (Conf.acornYoutubeOptions.test_number_of_reqs_per_dc) {
				YoutubeData.PrintNumReqsPerDc();
				return;
			}

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

			MakeRequests();

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

	private static void MakeRequests() throws Exception {
		int numThreads = Conf.acornYoutubeOptions.num_threads;
		List<Thread> reqThreads = new ArrayList<Thread>();
		for (int i = 0; i < numThreads; i ++) {
			Thread t = new Thread(new ReqThread());
			reqThreads.add(t);
		}

		_AgreeOnStartTime();

		Cons.P("Making requests ...");
		for (Thread t: reqThreads)
			t.start();

		// Some requests never finishes when Cassandra fails to write or read.
		// Ignore them after maxWaitTime. All will be joined 2 seconds after they
		// are done.
		long maxWaitTime =
			(SimTime.GetStartSimulationTime() - System.currentTimeMillis())
			+ Conf.acornYoutubeOptions.simulation_time_dur_in_ms
			+ Conf.acornYoutubeOptions.read_req_delay_in_simulation_time_in_ms
			+ 2000;
		Cons.P("  maxWaitTime: %d ms", maxWaitTime);

		ProgMon.Start();
		Thread.sleep(maxWaitTime);

		int i = 0;
		for (Thread t: reqThreads) {
			t.join(0, 1);
			i ++;
		}

		ProgMon.Stop();
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
						DbWriteMeasureTime(r);
					} else {
						SimTime.SleepUntilSimulatedTime(r);
						DbReadMeasureTime(r);
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

	private static void DbWriteMeasureTime(YoutubeData.Req r) throws Exception {
		long begin = System.nanoTime();

		if (Conf.acornYoutubeOptions.replication_type.equals("full")) {
			Cass.WriteYoutubeRegular(r);
		} else {
			Cass.WriteYoutubePartial(r);
		}

		long end = System.nanoTime();
		// Note: These 2 can be merged, when you have some time left.
		LatMon.Write(end - begin);
		ProgMon.Write();
	}

	private static void DbReadMeasureTime(YoutubeData.Req r) throws Exception {
		long begin = System.nanoTime();
		if (Conf.acornYoutubeOptions.replication_type.equals("full")) {
			Cass.ReadYoutubeRegular(r);
		} else {
			_FetchOnDemand(r);
		}
		long end = System.nanoTime();
		LatMon.Read(end - begin);
		ProgMon.Read();
	}

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
			if (Conf.acornYoutubeOptions.use_acorn_server) {
				if (Cass.LocalDC().equals("us-east")) {
					long now = System.currentTimeMillis();
					startTime = now + maxLapTime * 5;
					Cass.WriteStartTime(startTime);
				} else {
					startTime = Cass.ReadStartTimeUntilSucceed();
				}
			} else {
				long now = System.currentTimeMillis();
				startTime = now + 100;
			}
			SimTime.SetStartSimulationTime(startTime);
		}
	}

	private static void _FetchOnDemand(YoutubeData.Req r) throws Exception {
		if (! Conf.acornYoutubeOptions.use_acorn_server)
			return;

		List<Row> rows = Cass.ReadYoutubePartial(r);
		if (rows == null) {
			// Read timeout. Report to the ProgMon and ignore for now.
			return;
		}
		if (rows.size() == 1)
			return;
		if (rows.size() != 0)
			throw new RuntimeException(String.format("Unexpected: rows.size()=%d", rows.size()));

		// Get a DC where the object is
		String dc = Cass.GetObjLoc(r.vid);
		if (dc == null) {
			ProgMon.ReadMissDc();
			return;
		}

		// Possible since the updates to acorn.*_obj_loc keyspace and acorn.*_pr
		// keyspace are asynchronous.
		rows = Cass.ReadYoutubePartial(r, dc);
		if (rows == null) {
			// Read timeout
			return;
		}
		if (rows.size() == 0) {
			ProgMon.ReadMissObj();
			return;
		}
		Row row = rows.get(0);
		String vid = row.getString("video_id");
		String videoUploader = row.getString("uid");
		Set<String> topics = row.getSet("topics", String.class);
		ByteBuffer extraData = row.getBytes("extra_data");
		Cass.WriteYoutubePartial(vid, videoUploader, topics, extraData);

		ProgMon.FetchOnDemand();
	}
}
