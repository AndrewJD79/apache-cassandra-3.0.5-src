import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.InterruptedException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.*;

import com.google.common.base.Joiner;


class Cass {
	private static Cluster _cluster;
	private static Session _sess;

	private static String _local_dc = null;
	private static List<String> _remote_dcs = null;
	private static List<String> _all_dcs = null;

	private static String _ks_name = null;
	private static String _table_name = "t0";
	// Attribute popularity keyspace. A table per attribute.
	private static String _ks_name_attr_pop = null;
	// Object location keyspace.
	private static String _ks_name_obj_loc  = null;

	public static void Init() {
		try (Cons.MT _ = new Cons.MT("Cass Init ...")) {
			// The default LoadBalancingPolicy is DCAwareRoundRobinPolicy, which
			// round-robins over the nodes of the local data center, which is exactly
			// what you want in this project.
			// http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/DCAwareRoundRobinPolicy.html
			_cluster = new Cluster.Builder()
				// Connect to the local Cassandra server
				.addContactPoints("127.0.0.1")

				// It says,
				//   [main] INFO com.datastax.driver.core.Cluster - New Cassandra host /54.177.212.255:9042 added
				//   [main] INFO com.datastax.driver.core.Cluster - New Cassandra host /127.0.0.1:9042 added
				// , which made me wonder if this connect to a remote region as well.
				// The public IP is on a different region.
				//
				// Specifying a white list doesn't seem to make any difference.
				//.withLoadBalancingPolicy(
				//		new WhiteListPolicy(new DCAwareRoundRobinPolicy.Builder().build()
				//			, Collections.singletonList(new InetSocketAddress("127.0.0.1", 9042))
				//		))
				//
				// It might not mean which nodes this client connects to.

				.build();

			_sess = _cluster.connect();
			Metadata metadata = _cluster.getMetadata();
			Cons.P("Connected to cluster '%s'.", metadata.getClusterName());

			_WaitUntilYouSee2DCs();

			// Cassandra doesn't like "-".
			_ks_name = "acorn_test";

			_ks_name_attr_pop = _ks_name + "_attr_pop";
			_ks_name_obj_loc  = _ks_name + "_obj_loc";

			// TODO: clean up
			//private static String _csync_table_name = "client_sync";
		} catch (Exception e) {
			System.err.println("Exception: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void _WaitUntilYouSee2DCs() throws InterruptedException {
		try (Cons.MT _ = new Cons.MT("Wait until you see 2 DCs ...")) {
			ResultSet rs = _sess.execute("select data_center from system.local;");
			// Note that calling rs.all() for the second time returns an empty List<>.
			List<Row> rs_all = rs.all();
			if (rs_all.size() != 1)
				throw new RuntimeException(String.format("Unexpcted: %d", rs.all().size()));

			_local_dc = rs_all.get(0).getString("data_center");
			Cons.P("Local DC: %s", _local_dc);

			Cons.Pnnl("Remote DCs:");
			boolean first = true;
			while (true) {
				rs = _sess.execute("select data_center from system.peers;");
				rs_all = rs.all();
				if (rs_all.size() == 1)
					break;

				if (first) {
					System.out.print(" ");
					first = false;
				}
				System.out.print(".");
				System.out.flush();
				Thread.sleep(100);
			}

			_remote_dcs = new ArrayList<String>();
			for (Row r: rs_all)
				_remote_dcs.add(r.getString("data_center"));
			for (String r: _remote_dcs)
				System.out.printf(" %s", r);
			System.out.printf("\n");

			_all_dcs = new ArrayList<String>();
			_all_dcs.add(_local_dc);
			for (String r: _remote_dcs)
				_all_dcs.add(r);
		}
	}

	public static void Close() {
		try (Cons.MT _ = new Cons.MT("Closing _sess and _cluster ...")) {
			_sess.close();
			_cluster.close();
		}
	}

	public static String LocalDC() {
		return _local_dc;
	}

	public static boolean SchemaExist() {
		// Check if the created table, that is created last, exists
		String q = String.format("select obj_id from %s.obj_loc limit 1", _ks_name_obj_loc);
		Statement s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
		try {
			_sess.execute(s);
			return true;
		} catch (com.datastax.driver.core.exceptions.InvalidQueryException e) {
			if (e.toString().matches("(.*)Keyspace (.*) does not exist")) {
				return false;
			}
			else if (e.toString().contains("unconfigured table")) {
				return false;
			}

			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		}
	}

	public static void CreateSchema() {
		// You want to make sure that each test is starting from a clean sheet.
		// - But it takes too much time like 20 secs. Reuse the schema and make
		//   object IDs unique across runs, using the datetime of the run.
		// - Drop the keyspaces when the schema changes. (It is re-created.)

		// https://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html
		try (Cons.MT _ = new Cons.MT("Creating schema ...")) {
			String q = null;
			// It takes about exactly the same time with ALL and LOCAL_ONE. I wonder
			// it's always ALL implicitly.
			//ConsistencyLevel cl = ConsistencyLevel.ALL;
			ConsistencyLevel cl = ConsistencyLevel.LOCAL_ONE;
			try {
				// Prepare datacenter query string
				StringBuilder q_dcs = new StringBuilder();
				for (String r: _all_dcs)
					q_dcs.append(String.format(", '%s' : 1", r));

				// Object keyspace
				{
					q = String.format("CREATE KEYSPACE %s WITH replication = {"
							+ " 'class' : 'NetworkTopologyStrategy'%s};"
							, _ks_name, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_sess.execute(s);
					// It shouldn't already exist. The keyspace name is supposed to be
					// unique for each run. No need to catch AlreadyExistsException.

					q = String.format("CREATE TABLE %s.%s"
							+ " (obj_id text"
							+ ", user text, topics set<text>"	// attributes
							+ ", PRIMARY KEY (obj_id)"				// Primary key is mandatory
							+ ");",
							_ks_name, _table_name);
					s = new SimpleStatement(q).setConsistencyLevel(cl);
					_sess.execute(s);
				}

				// Attribute popularity keyspace
				{
					q = String.format("CREATE KEYSPACE %s WITH replication = {"
							+ " 'class' : 'NetworkTopologyStrategy'%s};"
							, _ks_name_attr_pop, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_sess.execute(s);

					// These are periodically updated (broadcasted).
					for (String dc: _all_dcs) {
						q = String.format("CREATE TABLE %s.%s_user (user_id text, PRIMARY KEY (user_id));"
								, _ks_name_attr_pop, dc.replace("-", "_"));
						s = new SimpleStatement(q).setConsistencyLevel(cl);
						_sess.execute(s);

						q = String.format("CREATE TABLE %s.%s_topic (topic text, PRIMARY KEY (topic));"
								, _ks_name_attr_pop, dc.replace("-", "_"));
						s = new SimpleStatement(q).setConsistencyLevel(cl);
						_sess.execute(s);
					}
				}

				// Object location keyspace.
				{
					q = String.format("CREATE KEYSPACE %s WITH replication = {"
							+ " 'class' : 'NetworkTopologyStrategy'%s};"
							, _ks_name_obj_loc, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_sess.execute(s);

					// The CLs of the operations on this table determines the consistency
					// model of the applications.
					q = String.format("CREATE TABLE %s.obj_loc (obj_id text, locations set<text>, PRIMARY KEY (obj_id));"
							, _ks_name_obj_loc);
					s = new SimpleStatement(q).setConsistencyLevel(cl);
					_sess.execute(s);
				}
			} catch (com.datastax.driver.core.exceptions.DriverException e) {
				Cons.P("Exception %s. query=[%s]", e, q);
				throw e;
			}
		}

		// Note: global sync can be implemented by east writing something with CL
		// ALL and everyone, including east itself, keeps reading the value with CL
		// LOCAL_ONE until it sees the value.
		//
		// Note: agreeing on a future time can be implemented similarily. east
		// posting a near future time with CL ALL and make sure the time is well in
		// the future (like 1 sec after).
	}

	public static void WaitForSchemaCreation()
		throws InterruptedException {
		try (Cons.MT _ = new Cons.MT("Waiting for the schema creation ...")) {
			// Select data from the last created table with a CL local_ONE until
			// there is no exception.
			String q = String.format("select obj_id from %s.obj_loc", _ks_name_obj_loc);
			Statement s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
			Cons.Pnnl("Checking:");
			boolean first = true;
			while (true) {
				try {
					_sess.execute(s);
					break;
				} catch (com.datastax.driver.core.exceptions.InvalidQueryException e) {
					char error_code = '-';
					// Keyspace acorn_test_160510_011454_obj_loc does not exist.
					if (e.toString().matches("(.*)Keyspace (.*) does not exist")) {
						error_code = 'k';
					}
					// unconfigured table
					else if (e.toString().contains("unconfigured table")) {
						error_code = 'u';
					}

					if (error_code == '-') {
						Cons.P("Exception=[%s] query=[%s]", e, q);
						throw e;
					}

					if (first) {
						System.out.printf(" ");
						first = false;
					}
					System.out.printf("%c", error_code);
					System.out.flush();
					Thread.sleep(100);
				} catch (com.datastax.driver.core.exceptions.DriverException e) {
					Cons.P("Exception=[%s] query=[%s]", e, q);
					throw e;
				}
			}
			System.out.printf(" exists\n");
		}
	}

	static public void InsertRecord(String obj_id, String user, Set<String> topics) {
		StringBuilder qs_topics = new StringBuilder();
		for (String t: topics) {
			if (qs_topics.length() > 0)
				qs_topics.append(", ");
			qs_topics.append(String.format("'%s'", t));
		}

		String q = String.format(
				"INSERT INTO %s.%s (obj_id, user, topics) VALUES ('%s', '%s', {%s});"
				, _ks_name, _table_name,
				obj_id, user, qs_topics);
		_sess.execute(q);
	}

	static public void SelectRecordLocalUntilSucceed(String obj_id) throws InterruptedException {
		try (Cons.MT _ = new Cons.MT("Select record %s ", obj_id)) {
			// Select data from the last created table with a CL local_ONE until
			// succeed.
			String q = String.format("select obj_id from %s.%s where obj_id='%s'"
					, _ks_name, _table_name, obj_id);
			Statement s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
			Cons.Pnnl("Checking: ");
			while (true) {
				try {
					ResultSet rs = _sess.execute(s);
					List<Row> rows = rs.all();
					if (rows.size() == 0) {
						System.out.printf(".");
						System.out.flush();
						Thread.sleep(10);
					} else if (rows.size() == 1) {
						System.out.printf(" found one\n");
						break;
					} else {
						throw new RuntimeException(String.format("Unexpcted: rows.size()=%d", rows.size()));
					}
				} catch (com.datastax.driver.core.exceptions.DriverException e) {
					Cons.P("Exception=[%s] query=[%s]", e, q);
					throw e;
				}
			}
		}
	}

	static public List<Row> SelectRecordLocal(String obj_id) {
		String q = String.format("select obj_id from %s.%s where obj_id='%s'"
				, _ks_name, _table_name, obj_id);
		Statement s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
		try {
			ResultSet rs = _sess.execute(s);
			List<Row> rows = rs.all();
			return rows;
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		}
	}



//	static public void Sync(int tid)
//		throws java.net.UnknownHostException, java.lang.InterruptedException {
//		long bt = System.currentTimeMillis();
//		System.out.printf("\nSync tid=%d ", tid);
//		System.out.flush();
//		long timeout_milli = 30000;
//		Sync0(tid, timeout_milli);
//		Sync1(tid, timeout_milli);
//		long et = System.currentTimeMillis();
//		System.out.printf(" %d ms\n", et - bt);
//	}
//
//	static private void Sync0(int tid, long timeout_milli)
//		throws java.net.UnknownHostException, java.lang.InterruptedException {
//		if (Conf.dc.equals("DC1")) {
//			String q = String.format(
//					"INSERT INTO %s.%s (exp_id, tid, hn, time) VALUES ('%s', %d, '%s', %d);",
//					_meta_ks_name, _csync_table_name,
//					Conf.dt_begin, tid, Util.Hostname(), System.currentTimeMillis());
//			_sess.execute(q);
//		} else if (Conf.dc.equals("DC0")) {
//			long bt = System.currentTimeMillis();
//			String q = String.format(
//					"SELECT * FROM %s.%s WHERE exp_id='%s' AND tid=%d;",
//					_meta_ks_name, _csync_table_name, Conf.dt_begin, tid);
//			while (true) {
//				ResultSet rs = _sess.execute(q);
//				boolean got_it = false;
//				for (Row r: rs.all()) {
//					String hn = r.getString("hn");
//					//long t = r.getLong("time");
//					if (hn.equals(Conf.hns[1])) {
//						got_it = true;
//						break;
//					}
//				}
//				if (got_it)
//					break;
//
//				if (System.currentTimeMillis() - bt > timeout_milli) {
//					System.out.printf(" ");
//					throw new RuntimeException("Timed out waiting getting 1 row");
//				}
//
//				System.out.printf(".");
//				System.out.flush();
//				Thread.sleep(100);
//			}
//		} else
//			throw new RuntimeException("unknown dc: " + Conf.dc);
//	}
//
//	static private void Sync1(int tid, long timeout_milli)
//		throws java.net.UnknownHostException, java.lang.InterruptedException {
//		if (Conf.dc.equals("DC0")) {
//			String q = String.format(
//					"INSERT INTO %s.%s (exp_id, tid, hn, time) VALUES ('%s', %d, '%s', %d);",
//					_meta_ks_name, _csync_table_name,
//					Conf.dt_begin, tid, Util.Hostname(), System.currentTimeMillis());
//			_sess.execute(q);
//		} else if (Conf.dc.equals("DC1")) {
//			long bt = System.currentTimeMillis();
//			String q = String.format(
//					"SELECT * FROM %s.%s WHERE exp_id='%s' AND tid=%d;",
//					_meta_ks_name, _csync_table_name, Conf.dt_begin, tid);
//			while (true) {
//				ResultSet rs = _sess.execute(q);
//				boolean got_it = false;
//				for (Row r: rs.all()) {
//					String hn = r.getString("hn");
//					//long t = r.getLong("time");
//					if (hn.equals(Conf.hns[0])) {
//						got_it = true;
//						break;
//					}
//				}
//				if (got_it)
//					break;
//
//				if (System.currentTimeMillis() - bt > timeout_milli) {
//					System.out.printf(" ");
//					throw new RuntimeException("Timed out waiting getting 1 row");
//				}
//
//				System.out.printf(".");
//				System.out.flush();
//				Thread.sleep(100);
//			}
//		} else
//			throw new RuntimeException("unknown dc: " + Conf.dc);
//	}
//
//	static public void Insert(int tid)
//		throws java.net.UnknownHostException {
//		System.out.printf("  Insert ...");
//		System.out.flush();
//		long bt = System.currentTimeMillis();
//		String q0 = String.format(
//				"INSERT INTO %s.%s (exp_id, tid, pr_tdcs) "
//				+ "VALUES ('%s', %d, {'%s', '%s'});",
//				_ks_name, _table_name, Conf.dt_begin, tid,
//				Conf.hns[0], Conf.hns[1]);
//		_sess.execute(q0);
//		long et = System.currentTimeMillis();
//		System.out.printf(" %d ms\n", et - bt);
//	}
//
//	static public ResultSet Execute(String q) {
//		return _sess.execute(q);
//	}
//
//	static public void InsertToDC0(int tid)
//		throws java.net.UnknownHostException {
//		System.out.printf("  InsertToDC0 ...");
//		System.out.flush();
//		long bt = System.currentTimeMillis();
//		String q0 = String.format(
//				"INSERT INTO %s.%s (exp_id, tid, pr_tdcs) "
//				+ "VALUES ('%s', %d, {'%s'});",
//				_ks_name, _table_name, Conf.dt_begin, tid,
//				Conf.hns[0]);
//		_sess.execute(q0);
//		long et = System.currentTimeMillis();
//		System.out.printf(" %d ms\n", et - bt);
//	}
//
//	static public int SelectLocal(int tid) {
//		String q0;
//		q0 = String.format(
//				"SELECT * FROM %s.%s WHERE exp_id='%s' AND tid=%d;",
//				_ks_name, _table_name, Conf.dt_begin, tid);
//		ResultSet rs = _sess.execute(q0);
//		List<Row> rows = rs.all();
//		return rows.size();
//	}
//
//	static public int SelectFetchOnDemand(int tid, String s_dc, boolean sync) {
//		ResultSet rs = _sess.execute(String.format(
//				"SELECT * FROM %s.%s WHERE exp_id='%s' AND tid=%d AND pr_sdc='%s' AND pr_sync=%s;",
//				_ks_name, _table_name, Conf.dt_begin, tid, s_dc, sync ? "true" : "false"));
//		List<Row> rows = rs.all();
//
//		return rows.size();
//	}
//
//	private static String _IPtoDC(String ip)
//		throws java.io.FileNotFoundException, java.io.IOException {
//		String fn = Conf.CassandraSrcDn() + "/conf/cassandra-topology.properties";
//		BufferedReader br = new BufferedReader(new FileReader(fn));
//		while (true) {
//			String line = br.readLine();
//			if (line == null)
//				break;
//			if (line.length() == 0)
//				continue;
//			if (line.charAt(0) == '#')
//				continue;
//			String[] t = line.split("=|:");
//			if (t.length == 3) {
//				//System.out.printf("%s %s %s\n", t[0], t[1], t[2]);
//				if (ip.equals(t[0]))
//					return t[1];
//			}
//		}
//		if (true) throw new RuntimeException("Unknown ip " + ip);
//		return "";
//	}
//
//	private static ResultSet _RunQuery(String q)
//		throws InterruptedException {
//		while (true) {
//			try {
//				return _sess.execute(q);
//			} catch (DriverException e) {
//				System.err.println("Error during query: " + e.getMessage());
//				e.printStackTrace();
//				System.out.printf("Retrying in 5 sec...\n");
//				Thread.sleep(5000);
//			}
//		}
//	}
//
//	private static ResultSet _RunQuery(Query q)
//		throws InterruptedException {
//		while (true) {
//			try {
//				return _sess.execute(q);
//			} catch (DriverException e) {
//				System.err.println("Error during query: " + e.getMessage());
//				e.printStackTrace();
//				System.out.printf("Retrying in 5 sec...\n");
//				Thread.sleep(5000);
//			}
//		}
//	}
}
