import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.InterruptedException;
import java.util.List;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.*;


class Cass {
	private static String _ks_name = "pr_ondemand_client";
	private static String _table_name = "t0";
	private static String _meta_ks_name = "pr_meta";
	private static String _csync_table_name = "client_sync";

	private static Cluster _cluster;
	private static Session _sess;

	public static void Init() {
		try {
			System.out.printf("Cass Init ...\n");
			long bt = System.currentTimeMillis();

			String ip = Util.GetEth0IP();

			// TODO: make sure always connect to the local DC
			_cluster = new Cluster.Builder()
				.addContactPoints(ip)
				.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy(_IPtoDC(ip)))
				.build();
			_sess = _cluster.connect();
			Metadata metadata = _cluster.getMetadata();
			System.out.println(String.format("  Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts()));
			
			long et = System.currentTimeMillis();
			System.out.printf("  %d ms\n", et - bt);
		} catch (Exception e) {
			System.err.println("Exception: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static void Close() {
		System.out.printf("Closing _sess and _cluster ... ");
		long bt = System.currentTimeMillis();
		_sess.shutdown();
		_cluster.shutdown();
		long et = System.currentTimeMillis();
		System.out.printf("%d ms\n", et - bt);
	}


	public static void CreateSchema() {
		System.out.printf("Creating schema ... ");
		long bt = System.currentTimeMillis();
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("CREATE KEYSPACE " + _ks_name + " WITH replication = { "
					+ "'class' : 'NetworkTopologyStrategy'");
			for (int i = 0; i < Conf.NumDCs(); i ++)
				sb.append(String.format(", 'DC%d' : 1", i));
			sb.append(" }; ");

			_sess.execute(sb.toString());
		} catch (AlreadyExistsException e) {}
		//_sess.execute("use " + _ks_name + ";");

		try {
			StringBuilder sb = new StringBuilder();
			sb.append("CREATE KEYSPACE " + _meta_ks_name + " WITH replication = { "
					+ "'class' : 'NetworkTopologyStrategy'");
			for (int i = 0; i < Conf.NumDCs(); i ++)
				sb.append(String.format(", 'DC%d' : 1", i));
			sb.append(" }; ");

			_sess.execute(sb.toString());
		} catch (AlreadyExistsException e) {}

		try {
			_sess.execute(String.format("CREATE TABLE %s.%s ("
						+ "exp_dt text, "	// experiment date time
						+ "tid int, "						// 4
						+ "PRIMARY KEY (exp_dt, tid) "
						+ "); ",
						_ks_name, _table_name));
		} catch (AlreadyExistsException e) {}

		try {
			_sess.execute(String.format("CREATE TABLE %s.%s ("
					+ "exp_dt text, "	// experiment date time
					+ "tid int, "
					+ "hn text, "
					+ "time bigint, "
					+ "PRIMARY KEY (exp_dt, tid) "
					+ "); ",
					_meta_ks_name, _csync_table_name));
		} catch (AlreadyExistsException e) {}

		long et = System.currentTimeMillis();
		System.out.printf("%d ms\n", et - bt);
	}

	public static void WaitForSchemaCreation()
		throws InterruptedException {
		System.out.printf("Waiting for schema creation ");
		long bt = System.currentTimeMillis();

		String q = "SELECT * FROM " + _meta_ks_name + "." + _csync_table_name + ";";

		while (true) {
			try {
				ResultSet rs = _sess.execute(q);
				break;
			} catch (com.datastax.driver.core.exceptions.InvalidQueryException e) {
				if (e.getMessage().equals("Keyspace " + _meta_ks_name + " does not exist"))
					;
				else if (e.getMessage().equals("unconfigured columnfamily " + _csync_table_name))
					;
				else {
					throw e;
				}
			}

			System.out.printf(".");
			System.out.flush();
			Thread.sleep(100);
		}

		long et = System.currentTimeMillis();
		System.out.printf(" %d ms\n", et - bt);
	}

	static public void Sync(int tid)
		throws java.net.UnknownHostException, java.lang.InterruptedException {
		long bt = System.currentTimeMillis();
		System.out.printf("\nSync tid=%d ", tid);
		System.out.flush();
		long timeout_milli = 30000;
		Sync0(tid, timeout_milli);
		Sync1(tid, timeout_milli);
		long et = System.currentTimeMillis();
		System.out.printf(" %d ms\n", et - bt);
	}

	static private void Sync0(int tid, long timeout_milli)
		throws java.net.UnknownHostException, java.lang.InterruptedException {
		if (Conf.dc.equals("DC1")) {
			String q = String.format(
					"INSERT INTO %s.%s (exp_dt, tid, hn, time) VALUES ('%s', %d, '%s', %d);",
					_meta_ks_name, _csync_table_name,
					Conf.dt_begin, tid, Util.Hostname(), System.currentTimeMillis());
			_sess.execute(q);
		} else if (Conf.dc.equals("DC0")) {
			long bt = System.currentTimeMillis();
			String q = String.format(
					"SELECT * FROM %s.%s WHERE exp_dt='%s' AND tid=%d;",
					_meta_ks_name, _csync_table_name, Conf.dt_begin, tid);
			while (true) {
				ResultSet rs = _sess.execute(q);
				boolean got_it = false;
				for (Row r: rs.all()) {
					String hn = r.getString("hn");
					//long t = r.getLong("time");
					if (hn.equals(Conf.hns[1])) {
						got_it = true;
						break;
					}
				}
				if (got_it)
					break;

				if (System.currentTimeMillis() - bt > timeout_milli) {
					System.out.printf(" ");
					throw new RuntimeException("Timed out waiting getting 1 row");
				}

				System.out.printf(".");
				System.out.flush();
				Thread.sleep(100);
			}
		} else
			throw new RuntimeException("unknown dc: " + Conf.dc);
	}

	static private void Sync1(int tid, long timeout_milli)
		throws java.net.UnknownHostException, java.lang.InterruptedException {
		if (Conf.dc.equals("DC0")) {
			String q = String.format(
					"INSERT INTO %s.%s (exp_dt, tid, hn, time) VALUES ('%s', %d, '%s', %d);",
					_meta_ks_name, _csync_table_name,
					Conf.dt_begin, tid, Util.Hostname(), System.currentTimeMillis());
			_sess.execute(q);
		} else if (Conf.dc.equals("DC1")) {
			long bt = System.currentTimeMillis();
			String q = String.format(
					"SELECT * FROM %s.%s WHERE exp_dt='%s' AND tid=%d;",
					_meta_ks_name, _csync_table_name, Conf.dt_begin, tid);
			while (true) {
				ResultSet rs = _sess.execute(q);
				boolean got_it = false;
				for (Row r: rs.all()) {
					String hn = r.getString("hn");
					//long t = r.getLong("time");
					if (hn.equals(Conf.hns[0])) {
						got_it = true;
						break;
					}
				}
				if (got_it)
					break;

				if (System.currentTimeMillis() - bt > timeout_milli) {
					System.out.printf(" ");
					throw new RuntimeException("Timed out waiting getting 1 row");
				}

				System.out.printf(".");
				System.out.flush();
				Thread.sleep(100);
			}
		} else
			throw new RuntimeException("unknown dc: " + Conf.dc);
	}

	static public void Insert(int tid)
		throws java.net.UnknownHostException {
		System.out.printf("  Insert ...");
		System.out.flush();
		long bt = System.currentTimeMillis();
		String q0 = String.format(
				"INSERT INTO %s.%s (exp_dt, tid, pr_tdcs) "
				+ "VALUES ('%s', %d, {'%s', '%s'});",
				_ks_name, _table_name, Conf.dt_begin, tid,
				Conf.hns[0], Conf.hns[1]);
		_sess.execute(q0);
		long et = System.currentTimeMillis();
		System.out.printf(" %d ms\n", et - bt);
	}

	static public ResultSet Execute(String q) {
		return _sess.execute(q);
	}

	static public void InsertToDC0(int tid)
		throws java.net.UnknownHostException {
		System.out.printf("  InsertToDC0 ...");
		System.out.flush();
		long bt = System.currentTimeMillis();
		String q0 = String.format(
				"INSERT INTO %s.%s (exp_dt, tid, pr_tdcs) "
				+ "VALUES ('%s', %d, {'%s'});",
				_ks_name, _table_name, Conf.dt_begin, tid,
				Conf.hns[0]);
		_sess.execute(q0);
		long et = System.currentTimeMillis();
		System.out.printf(" %d ms\n", et - bt);
	}

	static public int SelectLocal(int tid) {
		String q0;
		q0 = String.format(
				"SELECT * FROM %s.%s WHERE exp_dt='%s' AND tid=%d;",
				_ks_name, _table_name, Conf.dt_begin, tid);
		ResultSet rs = _sess.execute(q0);
		List<Row> rows = rs.all();
		return rows.size();
	}

	static public int SelectFetchOnDemand(int tid, String s_dc, boolean sync) {
		ResultSet rs = _sess.execute(String.format(
				"SELECT * FROM %s.%s WHERE exp_dt='%s' AND tid=%d AND pr_sdc='%s' AND pr_sync=%s;",
				_ks_name, _table_name, Conf.dt_begin, tid, s_dc, sync ? "true" : "false"));
		List<Row> rows = rs.all();

		return rows.size();
	}

	private static String _IPtoDC(String ip)
		throws java.io.FileNotFoundException, java.io.IOException {
		String fn = Conf.CassandraSrcDn() + "/conf/cassandra-topology.properties";
		BufferedReader br = new BufferedReader(new FileReader(fn));
		while (true) {
			String line = br.readLine();
			if (line == null)
				break;
			if (line.length() == 0)
				continue;
			if (line.charAt(0) == '#')
				continue;
			String[] t = line.split("=|:");
			if (t.length == 3) {
				//System.out.printf("%s %s %s\n", t[0], t[1], t[2]);
				if (ip.equals(t[0]))
					return t[1];
			}
		}
		if (true) throw new RuntimeException("Unknown ip " + ip);
		return "";
	}

	private static ResultSet _RunQuery(String q)
		throws InterruptedException {
		while (true) {
			try {
				return _sess.execute(q);
			} catch (DriverException e) {
				System.err.println("Error during query: " + e.getMessage());
				e.printStackTrace();
				System.out.printf("Retrying in 5 sec...\n");
				Thread.sleep(5000);
			}
		}
	}

	private static ResultSet _RunQuery(Query q)
		throws InterruptedException {
		while (true) {
			try {
				return _sess.execute(q);
			} catch (DriverException e) {
				System.err.println("Error during query: " + e.getMessage());
				e.printStackTrace();
				System.out.printf("Retrying in 5 sec...\n");
				Thread.sleep(5000);
			}
		}
	}
}
