import java.net.UnknownHostException;
import com.datastax.driver.core.*;

public class FetchOnDemand {

//	// test id
//	private static int _tid = 0;
//
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
//			System.out.printf("    got new data in %d ms from FetchOnDemand\n", et - bt);
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

			CreateSchema();

			// TODO: wait till All DCs see the schema. check every 0.1 sec.

			// TODO
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
		if (Cass.LocalDC().equals("us-east"))
			Cass.CreateSchema();

		Cass.WaitForSchemaCreation();
	}
}
