import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.InterruptedException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.*;

import com.google.common.base.Joiner;


class Cass {
	private static Cluster _cluster;

	// Session instances are thread-safe and usually a single instance is enough per application.
	// http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/Session.html
	private static Session _sess;

	private static String _local_dc = null;
	private static List<String> _remote_dcs = null;
	private static List<String> _all_dcs = null;

	// Partial replication
	private static String _ks_pr = null;

	// Attribute popularity keyspace. A table per attribute.
	private static String _ks_attr_pop = null;
	// Object location keyspace.
	private static String _ks_obj_loc  = null;
	private static String _ks_sync = null;

	// For comparison
	private static String _ks_regular = null;

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

			String ks_prefix = "acorn";
			_ks_pr = ks_prefix + "_pr";
			_ks_attr_pop = ks_prefix + "_attr_pop";
			_ks_obj_loc  = ks_prefix + "_obj_loc";
			_ks_sync = ks_prefix + "_sync";
			_ks_regular = ks_prefix + "_regular";
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
		String q = String.format("select c0 from %s.t0 limit 1", _ks_regular);
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
							, _ks_pr, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_sess.execute(s);
					// It shouldn't already exist. The keyspace name is supposed to be
					// unique for each run. No need to catch AlreadyExistsException.

					q = String.format("CREATE TABLE %s.t0"
							+ " (obj_id text"
							+ ", user text, topics set<text>"	// attributes
							+ ", PRIMARY KEY (obj_id)"				// Primary key is mandatory
							+ ");",
							_ks_pr);
					s = new SimpleStatement(q).setConsistencyLevel(cl);
					_sess.execute(s);
				}

				// Attribute popularity keyspace
				{
					q = String.format("CREATE KEYSPACE %s WITH replication = {"
							+ " 'class' : 'NetworkTopologyStrategy'%s};"
							, _ks_attr_pop, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_sess.execute(s);

					// These are periodically updated (broadcasted). Cassandra doesn't like "-".
					for (String dc: _all_dcs) {
						q = String.format("CREATE TABLE %s.%s_user (user_id text, PRIMARY KEY (user_id));"
								, _ks_attr_pop, dc.replace("-", "_"));
						s = new SimpleStatement(q).setConsistencyLevel(cl);
						_sess.execute(s);

						q = String.format("CREATE TABLE %s.%s_topic (topic text, PRIMARY KEY (topic));"
								, _ks_attr_pop, dc.replace("-", "_"));
						s = new SimpleStatement(q).setConsistencyLevel(cl);
						_sess.execute(s);
					}
				}

				// Object location keyspace.
				{
					q = String.format("CREATE KEYSPACE %s WITH replication = {"
							+ " 'class' : 'NetworkTopologyStrategy'%s};"
							, _ks_obj_loc, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_sess.execute(s);

					// The CLs of the operations on this table determines the consistency
					// model of the applications.
					q = String.format("CREATE TABLE %s.obj_loc (obj_id text, locations set<text>, PRIMARY KEY (obj_id));"
							, _ks_obj_loc);
					s = new SimpleStatement(q).setConsistencyLevel(cl);
					_sess.execute(s);
				}

				// Sync keyspace for testing purpose.
				{
					q = String.format("CREATE KEYSPACE %s WITH replication = {"
							+ " 'class' : 'NetworkTopologyStrategy'%s};"
							, _ks_sync, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_sess.execute(s);

					// The CLs of the operations on this table determines the consistency
					// model of the applications.
					q = String.format("CREATE TABLE %s.t0 (sync_id text, PRIMARY KEY (sync_id));"
							, _ks_sync);
					s = new SimpleStatement(q).setConsistencyLevel(cl);
					_sess.execute(s);
				}

				// A regular keyspace for comparison
				{
					q = String.format("CREATE KEYSPACE %s WITH replication = {"
							+ " 'class' : 'NetworkTopologyStrategy'%s};"
							, _ks_regular, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_sess.execute(s);

					// The CLs of the operations on this table determines the consistency
					// model of the applications.
					q = String.format("CREATE TABLE %s.t0 (c0 text, c1 blob, PRIMARY KEY (c0));"
							, _ks_regular);
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
			// Select data from the last created table with a CL LOCAL_ONE until
			// there is no exception.
			String q = String.format("select c0 from %s.t0 limit 1", _ks_regular);
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
			ExecutionBarrier();
		}
	}

	static public void InsertRecordPartial(String obj_id, String user, Set<String> topics) {
		String q = null;
		try {
			q = String.format(
					"INSERT INTO %s.t0 (obj_id, user, topics) VALUES ('%s', '%s', {%s});"
					, _ks_pr, obj_id, user
					, String.join(", ", topics.stream().map(t -> String.format("'%s'", t)).collect(Collectors.toList())));
			_sess.execute(q);
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		}
	}

	static public void SelectRecordLocalUntilSucceed(String obj_id) throws InterruptedException {
		try (Cons.MT _ = new Cons.MT("Select record %s ", obj_id)) {
			// Select data from the last created table with a CL local_ONE until
			// succeed.
			String q = String.format("select obj_id from %s.t0 where obj_id='%s'"
					, _ks_pr, obj_id);
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
		// Note: Must do select * to have all attributes processed inside Cassandra server
		String q = String.format("select * from %s.t0 where obj_id='%s'"
				, _ks_pr, obj_id);
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

	static private int _barrier_id = 0;

	// Wait until east and west gets here
	static public void ExecutionBarrier() throws InterruptedException {
		String q = null;
		try {
			Cons.Pnnl("Execution barrier");
			long bt = System.currentTimeMillis();

			// Write us-(local_dc)-(exp_id)-(sync_id) with CL One. CL doesn't matter it
			// will propagate eventually.
			q = String.format("Insert into %s.t0 (sync_id) values ('%s-%s-%d');" ,
					_ks_sync, LocalDC(), Conf.ExpID(), _barrier_id);
			Statement s = new SimpleStatement(q);
			_sess.execute(s);

			// Keep reading us-(remote_dc)-(exp_id)-(sync_id) with CL LOCAL_ONE until
			// it sees the message from the other side.
			String peer_dc;
			if (LocalDC().equals("us-east")) {
				peer_dc = "us-west";
			} else {
				peer_dc = "us-east";
			}
			q = String.format("select sync_id from %s.t0 where sync_id='%s-%s-%d';" ,
					_ks_sync, peer_dc, Conf.ExpID(), _barrier_id);
			s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
			boolean first = true;
			while (true) {
				ResultSet rs = _sess.execute(s);
				List<Row> rows = rs.all();
				if (rows.size() == 0) {
					if (first) {
						System.out.printf(" ");
						first = false;
					}
					System.out.printf(".");
					System.out.flush();
					Thread.sleep(100);
				} else if (rows.size() == 1) {
					break;
				} else
					throw new RuntimeException(String.format("Unexpected: rows.size()=%d", rows.size()));

				if (System.currentTimeMillis() - bt > 10000) {
					System.out.printf("\n");
					throw new RuntimeException("Execution barrier wait timed out :(");
				}
			}

			_barrier_id ++;

			System.out.printf(" took %d ms\n", System.currentTimeMillis() - bt);
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		}
	}

	static public void InsertRandomToRegular(String obj_id, int recSize) throws InterruptedException {
		try {
			// http://ac31004.blogspot.com/2014/03/saving-image-in-cassandra-blob-field.html

			// http://stackoverflow.com/questions/5683206/how-to-create-an-array-of-20-random-bytes
			byte[] b = new byte[recSize];
			new Random().nextBytes(b);
			ByteBuffer bb = ByteBuffer.wrap(b);

			PreparedStatement ps = _sess.prepare(
					String.format("insert into %s.t0 (c0, c1) values (?,?)", _ks_regular));
			BoundStatement bs = new BoundStatement(ps);
			_sess.execute(bs.bind(obj_id, bb));
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s]", e);
			throw e;
		}
	}

	static public List<Row> SelectFromRegular(ConsistencyLevel cl, String obj_id) {
		String q = String.format("select * from %s.t0 where c0='%s'"
				, _ks_regular, obj_id);
		try {
			Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
			ResultSet rs = _sess.execute(s);
			List<Row> rows = rs.all();
			if (rows.size() != 1)
				throw new RuntimeException(String.format("Unexpcted: rows.size()=%d", rows.size()));
			return rows;
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		}
	}

	static public long SelectCountFromRegular(ConsistencyLevel cl, String objId0, String objId1) {
		String q = String.format("select count(*) from %s.t0 where c0 in ('%s', '%s')"
				, _ks_regular, objId0, objId1);
		try {
			Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
			ResultSet rs = _sess.execute(s);
			List<Row> rows = rs.all();
			if (rows.size() != 1)
				throw new RuntimeException(String.format("Unexpcted: rows.size()=%d", rows.size()));
			Row r = rows.get(0);
			return r.getLong(0);
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		}
	}
}
