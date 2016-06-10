import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.lang.InterruptedException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.*;

import com.google.common.base.Joiner;


class Cass {
	// What Cassandra Ec2Snitch returns
	private static String _localDc = null;

	private static List<String> _remoteDCs = null;
	private static List<String> _allDCs = null;

	// Partial replication
	private static String _ks_pr = null;

	// Attribute popularity keyspace. A table per attribute.
	private static String _ks_attr_pop = null;
	// Object location keyspace.
	private static String _ks_obj_loc  = null;
	private static String _ks_exe_barrier = null;
	private static String _ks_exp_meta = null;

	// For comparison
	private static String _ks_regular = null;

	public static void Init() throws Exception {
		try (Cons.MT _ = new Cons.MT("Cass Init ...")) {
			_WaitUntilYouSeeAllDCs();

			String ks_prefix = "acorn";
			_ks_pr          = ks_prefix + "_pr";
			_ks_attr_pop    = ks_prefix + "_attr_pop";
			_ks_obj_loc     = ks_prefix + "_obj_loc";
			_ks_exe_barrier = ks_prefix + "_exe_barrier";
			_ks_exp_meta    = ks_prefix + "_exp_meta";
			_ks_regular     = ks_prefix + "_regular";
		}
	}

	private static void _WaitUntilYouSeeAllDCs() throws Exception {
		try (Cons.MT _ = new Cons.MT("Wait until you see all DCs ...")) {
			ResultSet rs = _GetSession().execute("select data_center from system.local;");
			// Note that calling rs.all() for the second time returns an empty List<>.
			List<Row> rows = rs.all();
			if (rows.size() != 1)
				throw new RuntimeException(String.format("Unexpcted: %d", rows.size()));

			_localDc = rows.get(0).getString("data_center");
			Cons.P("Local DC: %s", _localDc);

			Cons.Pnnl("Remote DCs:");
			long bt = System.currentTimeMillis();
			boolean first = true;
			while (true) {
				rs = _GetSession().execute("select data_center from system.peers;");
				rows = rs.all();
				if (rows.size() == DC.remoteDCs.size())
					break;

				if (first) {
					System.out.print(" ");
					first = false;
				}
				System.out.print(".");
				System.out.flush();

				if (System.currentTimeMillis() - bt > 2000) {
					System.out.printf("\n");
					throw new RuntimeException("Time out :(");
				}

				Thread.sleep(100);
			}

			_remoteDCs = new ArrayList<String>();
			for (Row r: rows)
				_remoteDCs.add(r.getString("data_center"));
			for (String rDc: _remoteDCs)
				System.out.printf(" %s", rDc);
			System.out.printf("\n");

			_allDCs = new ArrayList<String>();
			_allDCs.add(_localDc);
			for (String rDc: _remoteDCs)
				_allDCs.add(rDc);

			// Prepare remote sessions in parallel for later. It takes about 800 ms
			// per each.
			try (Cons.MT _1 = new Cons.MT("Prepareing remote sessions ...")) {
				List<Thread> threads = new ArrayList<Thread>();
				for (String rDc: _remoteDCs) {
					Thread t = new Thread(new ThreadGetSession(rDc));
					t.start();
					threads.add(t);
				}
				for (Thread t: threads)
					t.join();
			}
		}
	}

	private static class ThreadGetSession implements Runnable
	{
		private String dc;

		ThreadGetSession(String dc) {
			this.dc = dc;
		}

		public void run() {
			try {
				_GetSession(dc);
			} catch (Exception e) {
				System.out.printf("Exception: %s\n%s\n", e, Util.GetStackTrace(e));
				System.exit(1);
			}
		}
	}

	public static String LocalDC() {
		return _localDc;
	}

	public static boolean SchemaExist() throws Exception {
		// Check if the created table, that is created last, exists
		String q = String.format("select * from %s.t0 limit 1", _ks_regular);
		Statement s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
		try {
			_GetSession().execute(s);
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

	public static void CreateSchema() throws Exception {
		// You want to make sure that each test is starting from a clean sheet.
		// - But it takes too much time like 20 secs. Reuse the schema and make
		//   object IDs unique across runs, using the datetime of the run.
		// - Drop the keyspaces when the schema changes. (It is re-created.)

		// https://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html
		try (Cons.MT _ = new Cons.MT("Creating schema ...")) {
			String q = null;
			// It takes about exactly the same time with ALL and LOCAL_ONE. I wonder
			// it's always ALL implicitly for keyspace and table creation queries.
			//ConsistencyLevel cl = ConsistencyLevel.ALL;
			ConsistencyLevel cl = ConsistencyLevel.LOCAL_ONE;
			try {
				// Prepare datacenter query string
				StringBuilder q_dcs = new StringBuilder();
				for (String r: _allDCs)
					q_dcs.append(String.format(", '%s' : 1", r));

				// Object keyspace
				{
					q = String.format("CREATE KEYSPACE %s WITH replication = {"
							+ " 'class' : 'NetworkTopologyStrategy'%s};"
							, _ks_pr, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_GetSession().execute(s);
					// It shouldn't already exist. The keyspace name is supposed to be
					// unique for each run. No need to catch AlreadyExistsException.

					// A minimal schema to prove the concept.
					q = String.format("CREATE TABLE %s.t0"
							+ " (video_id text"			// YouTube video id. Primary key
							+ ", uid text"				// Attribute user. The uploader of the video.
							+ ", topics set<text>"	// Attribute topics
							+ ", extra_data blob"		// Extra data to make the record size configurable
							+ ", PRIMARY KEY (video_id)"	// Primary key is mandatory
							+ ");",
							_ks_pr);

					s = new SimpleStatement(q).setConsistencyLevel(cl);
					_GetSession().execute(s);
				}

				// Attribute popularity keyspace
				{
					q = String.format("CREATE KEYSPACE %s WITH replication = {"
							+ " 'class' : 'NetworkTopologyStrategy'%s};"
							, _ks_attr_pop, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_GetSession().execute(s);

					// These are periodically updated (broadcasted). Cassandra doesn't like "-".
					for (String dc: _allDCs) {
						q = String.format("CREATE TABLE %s.%s_user (user_id text, PRIMARY KEY (user_id));"
								, _ks_attr_pop, dc.replace("-", "_"));
						s = new SimpleStatement(q).setConsistencyLevel(cl);
						_GetSession().execute(s);

						q = String.format("CREATE TABLE %s.%s_topic (topic text, PRIMARY KEY (topic));"
								, _ks_attr_pop, dc.replace("-", "_"));
						s = new SimpleStatement(q).setConsistencyLevel(cl);
						_GetSession().execute(s);
					}
				}

				// Object location keyspace.
				{
					q = String.format("CREATE KEYSPACE %s WITH replication = {"
							+ " 'class' : 'NetworkTopologyStrategy'%s};"
							, _ks_obj_loc, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_GetSession().execute(s);

					q = String.format("CREATE TABLE %s.obj_loc (obj_id text, locations set<text>, PRIMARY KEY (obj_id));"
							, _ks_obj_loc);
					s = new SimpleStatement(q).setConsistencyLevel(cl);
					_GetSession().execute(s);
				}

				// Execution barrier keyspace
				{
					q = String.format("CREATE KEYSPACE %s WITH replication = {"
							+ " 'class' : 'NetworkTopologyStrategy'%s};"
							, _ks_exe_barrier, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_GetSession().execute(s);

					q = String.format("CREATE TABLE %s.t0 (barrier_id text, PRIMARY KEY (barrier_id));"
							, _ks_exe_barrier);
					s = new SimpleStatement(q).setConsistencyLevel(cl);
					_GetSession().execute(s);
				}

				// Experiment metadata keyspace
				{
					q = String.format("CREATE KEYSPACE %s WITH replication = {"
							+ " 'class' : 'NetworkTopologyStrategy'%s};"
							, _ks_exp_meta, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_GetSession().execute(s);

					q = String.format("CREATE TABLE %s.t0 (key text, value text, PRIMARY KEY (key));"
							, _ks_exp_meta);
					s = new SimpleStatement(q).setConsistencyLevel(cl);
					_GetSession().execute(s);
				}

				// A regular keyspace for comparison. Useful for a full replication
				// experiment.
				{
					q = String.format("CREATE KEYSPACE %s WITH replication = {"
							+ " 'class' : 'NetworkTopologyStrategy'%s};"
							, _ks_regular, q_dcs);
					Statement s = new SimpleStatement(q).setConsistencyLevel(cl);
					_GetSession().execute(s);

					q = String.format("CREATE TABLE %s.t0"
							+ " (video_id text"			// YouTube video id. Primary key
							+ ", uid text"					// Attribute user. The uploader of the video.
							+ ", topics set<text>"	// Attribute topics
							+ ", extra_data blob"		// Extra data to make the record size configurable
							+ ", PRIMARY KEY (video_id)"	// Primary key is mandatory
							+ ");",
							_ks_regular);
					s = new SimpleStatement(q).setConsistencyLevel(cl);
					_GetSession().execute(s);
				}
			} catch (com.datastax.driver.core.exceptions.DriverException e) {
				Cons.P("Exception %s. query=[%s]", e, q);
				throw e;
			}
		}
	}

	public static void WaitForSchemaCreation() throws Exception {
		try (Cons.MT _ = new Cons.MT("Waiting for the schema creation ...")) {
			// Select data from the last created table with a CL LOCAL_ONE until
			// there is no exception.
			String q = String.format("select * from %s.t0 limit 1", _ks_regular);
			Statement s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
			Cons.Pnnl("Checking:");
			boolean first = true;
			// Count the # of NoHostAvailableException(s)
			int nhaeCnt = 0;
			while (true) {
				try {
					_GetSession().execute(s);
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
					Thread.sleep(5000);
				} catch (com.datastax.driver.core.exceptions.DriverException e) {
					// This repeats. Not very often though.
					//
					// Datastax java driver message: WARN com.datastax.driver.core.RequestHandler - /54.177.190.122:9042 replied with server error (java.lang.IllegalArgumentException: Unknown CF 7dff11a0-1c6d-11e6-ad95-19553343aa25), defuncting connection.
					//
					// com.datastax.driver.core.exceptions.NoHostAvailableException: All host(s) tried for query failed (tried: /54.177.190.122:9042 (com.datastax.driver.core.exceptions.ServerError: An unexpected error occurred server side on /54.177.190.122:9042: java.lang.IllegalArgumentException: Unknown CF 7dff11a0-1c6d-11e6-ad95-19553343aa25))
					//
					// I might have to reconnect, instead of just retrying. Let's see how
					// it goes next time.

					boolean ok = false;
					if (e instanceof NoHostAvailableException) {
						nhaeCnt ++;
						if (nhaeCnt < 10) {
							ok = true;
							System.out.printf("h");
							System.out.flush();
							Thread.sleep(100);
						}
					}

					if (ok == false) {
						Cons.P("Exception=[%s] query=[%s]", e, q);
						throw e;
					}
				}
			}
			System.out.printf(" exists\n");

			Thread.sleep(1000);
			// The first barrier is more lenient.
			ExecutionBarrier(30000);
		}
	}

	private static PreparedStatement _ps0 = null;
	private static Object _ps0_sync = new Object();

	public static void WriteYoutubeRegular(YoutubeData.Req r) throws Exception {
		byte[] b = new byte[Conf.acornYoutubeOptions.youtube_extra_data_size];
		Random rand = ThreadLocalRandom.current();
		rand.nextBytes(b);
		ByteBuffer extraData = ByteBuffer.wrap(b);

		// Make once and reuse. Like test and test and set.
		if (_ps0 == null) {
			synchronized (_ps0_sync) {
				if (_ps0 == null) {
					_ps0 = _GetSession().prepare(
							String.format("INSERT INTO %s.t0 (video_id, uid, topics, extra_data) VALUES (?,?,?,?)", _ks_regular));
				}
			}
		}

		BoundStatement bs = new BoundStatement(_ps0);
		while (true) {
			try{
				if (Conf.acornYoutubeOptions.use_acorn_server) {
					_GetSession().execute(bs.bind(r.vid, r.videoUploader, new TreeSet<String>(r.topics), extraData));
				} else {
					Session s = _GetSession();
					bs.bind(r.vid, r.videoUploader, new TreeSet<String>(r.topics), extraData);
				}
				return;
			} catch (com.datastax.driver.core.exceptions.WriteTimeoutException e) {
				// com.datastax.driver.core.exceptions.WriteTimeoutException happens here.
				// Possibile explanations:
				// - EBS gets rate-limited.
				// - Cassandra gets overloaded. RX becomes almost like 100MB/sec.
				// http://www.datastax.com/dev/blog/cassandra-error-handling-done-right
				//
				// Report and retry.
				ProgMon.WriteTimeout();
			}
		}
	}

	public static void ReadYoutubeRegular(YoutubeData.Req r) throws Exception {
		// Note: Must do select * to have all attributes processed inside Cassandra
		// server. Doesn't matter for non acorn.*_pr keyspaces.
		String q = String.format("SELECT * from %s.t0 WHERE video_id='%s'", _ks_regular, r.vid);
		Statement s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
		try {
			while (true) {
				if (Conf.acornYoutubeOptions.use_acorn_server) {
					ResultSet rs = _GetSession().execute(s);
					List<Row> rows = rs.all();
					if (rows.size() == 1)
						return;
					// Hope it doesn't get stuck forever. If it happens, report retry count
					// and make it time out.
				} else {
					Session sess = _GetSession();
					return;
				}
			}
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		}
	}

	public static void WriteYoutubePartial(YoutubeData.Req r) throws Exception {
		byte[] b = new byte[Conf.acornYoutubeOptions.youtube_extra_data_size];
		Random rand = ThreadLocalRandom.current();
		rand.nextBytes(b);
		ByteBuffer extraData = ByteBuffer.wrap(b);

		WriteYoutubePartial(r.vid, r.videoUploader, new TreeSet<String>(r.topics), extraData);
	}

	private static PreparedStatement _ps1 = null;
	private static Object _ps1_sync = new Object();

	public static void WriteYoutubePartial(
			String vid, String videoUploader, Set<String> topics, ByteBuffer extraData) throws Exception
	{
		// Make once and reuse. Like test and test and set.
		if (_ps1 == null) {
			synchronized (_ps1_sync) {
				if (_ps1 == null) {
					_ps1 = _GetSession().prepare(
							String.format("INSERT INTO %s.t0 (video_id, uid, topics, extra_data) VALUES (?,?,?,?)", _ks_pr));
				}
			}
		}

		BoundStatement bs = new BoundStatement(_ps1);
		try{
			_GetSession().execute(bs.bind(vid, videoUploader, topics, extraData));
		} catch (com.datastax.driver.core.exceptions.WriteTimeoutException e) {
			// com.datastax.driver.core.exceptions.WriteTimeoutException happens here.
			// Possibile explanations:
			// - EBS gets rate-limited.
			// - Cassandra gets overloaded. RX becomes almost like 100MB/sec.
			// http://www.datastax.com/dev/blog/cassandra-error-handling-done-right
			//
			// Just report and don't bother retrying. A late write won't help reads
			// on the object.
			ProgMon.WriteTimeout();
		}
	}

	public static List<Row> ReadYoutubePartial(YoutubeData.Req r) throws Exception {
		return ReadYoutubePartial(r, null);
	}

	// For fetch-on-demand
	public static List<Row> ReadYoutubePartial(YoutubeData.Req r, String dc) throws Exception {
		// Note: Must do select * to have all attributes processed inside Cassandra
		// server. Doesn't matter for non acorn.*_pr keyspaces.
		String q = String.format("SELECT * from %s.t0 WHERE video_id='%s'", _ks_pr, r.vid);
		Statement s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
		try {
			ResultSet rs;
			if (dc == null) {
				rs = _GetSession().execute(s);
			} else {
				rs = _GetSession(dc).execute(s);
			}
			List<Row> rows = rs.all();
			return rows;
		} catch (com.datastax.driver.core.exceptions.NoHostAvailableException e) {
			ProgMon.ReadTimeout();
			return null;
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		}
	}

	static public void InsertRecordPartial(String obj_id, String user, Set<String> topics) throws Exception {
		String q = null;
		try {
			q = String.format(
					"INSERT INTO %s.t0 (obj_id, user, topics) VALUES ('%s', '%s', {%s});"
					, _ks_pr, obj_id, user
					, String.join(", ", topics.stream().map(t -> String.format("'%s'", t)).collect(Collectors.toList())));
			_GetSession().execute(q);
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		}
	}

	static public void SelectRecordLocalUntilSucceed(String obj_id) throws Exception {
		try (Cons.MT _ = new Cons.MT("Select record %s ", obj_id)) {
			// Select data from the last created table with a CL local_ONE until
			// succeed.
			String q = String.format("select obj_id from %s.t0 where obj_id='%s'"
					, _ks_pr, obj_id);
			Statement s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
			Cons.Pnnl("Checking: ");
			while (true) {
				try {
					ResultSet rs = _GetSession().execute(s);
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

	//static public List<Row> SelectRecordLocal(String objId) throws Exception {
	//	// Note: Must do select * to have all attributes processed inside Cassandra server
	//	String q = String.format("select * from %s.t0 where obj_id='%s'"
	//			, _ks_pr, objId);
	//	Statement s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
	//	try {
	//		ResultSet rs = _GetSession().execute(s);
	//		List<Row> rows = rs.all();
	//		return rows;
	//	} catch (com.datastax.driver.core.exceptions.DriverException e) {
	//		Cons.P("Exception=[%s] query=[%s]", e, q);
	//		throw e;
	//	}
	//}

	static public List<Row> SelectRecordRemote(String dc, String objId) throws Exception {
		// Note: Must do select * to have all attributes processed inside Cassandra server
		String q = String.format("select * from %s.t0 where obj_id='%s'"
				, _ks_pr, objId);
		Statement s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
		try {
			ResultSet rs = _GetSession(dc).execute(s);
			List<Row> rows = rs.all();
			return rows;
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		}
	}

	static private class ClusterSession {
		Cluster c;
		// Session instances are thread-safe and usually a single instance is enough per application.
		// http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/Session.html
		Session s;

		ClusterSession(Cluster c, Session s) {
			this.c = c;
			this.s = s;
		}
	}

	static private Map<String, ClusterSession> _mapDcSession = new TreeMap<String, ClusterSession>();

	static private Session _GetSession(String dc) throws Exception {
		// Cassandra Ec2Snitch thinks us-west-1 as us-west and us-west-2 as
		// us-west-2. https://issues.apache.org/jira/browse/CASSANDRA-4026
		String dcEc2Snitch = null;
		if (dc.endsWith("-1")) {
			dcEc2Snitch = dc.substring(0, dc.length() - 2);
		} else {
			dcEc2Snitch = dc;
		}

		ClusterSession cs = _mapDcSession.get(dcEc2Snitch);
		if (cs == null) {
			// The default LoadBalancingPolicy is DCAwareRoundRobinPolicy, which
			// round-robins over the nodes of the local data center, which is exactly
			// what you want in this project.
			// http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/DCAwareRoundRobinPolicy.html
			Cluster c = new Cluster.Builder()
				.addContactPoints(_GetDcPubIp(dcEc2Snitch))
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

			// Session instances are thread-safe and usually a single instance is enough per application.
			// http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/Session.html
			Session s = c.connect();
			//Metadata metadata = c.getMetadata();
			//Cons.P("Connected to cluster '%s'.", metadata.getClusterName());

			_mapDcSession.put(dcEc2Snitch, new ClusterSession(c, s));
			return s;
		} else {
			return cs.s;
		}
	}

	static private String _availabilityZone = null;

	static private Session _GetSession() throws Exception {
		if (_localDc != null)
			return _GetSession(_localDc);

		if (_availabilityZone == null) {
			Runtime r = Runtime.getRuntime();
			Process p = r.exec("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone");
			p.waitFor();
			BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			while ((line = b.readLine()) != null) {
				_availabilityZone = line;
			}
			b.close();
		}

		// Trim the last [a-z]
		return _GetSession(_availabilityZone.substring(0, _availabilityZone.length() - 1));
	}

	static private Map<String, String> _mapDcPubIp = new TreeMap<String, String>();

	static private String _GetDcPubIp(String dc) throws Exception {
		String ip = _mapDcPubIp.get(dc);
		if (ip == null) {
			File fn_jar = new File(AcornYoutube.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
			// /mnt/local-ssd0/work/apache-cassandra-3.0.5-src/acorn/test/AcornYoutube/target/AcornYoutube-0.1.jar
			String fn = String.format("%s/.run/dc-ip-map", fn_jar.getParentFile().getParentFile());
			Cons.P(fn);

			try (BufferedReader br = new BufferedReader(new FileReader(fn))) {
				String line;
				while ((line = br.readLine()) != null) {
					// us-east-1 54.160.118.182
					String[] t = line.split("\\s+");
					if (t.length !=2)
						throw new RuntimeException(String.format("Unexpcted format [%s]", line));

					// DC name to what Ec2Snitch thinks
					String dcEc2Snitch = t[0];
					if (dcEc2Snitch.endsWith("-1"))
						dcEc2Snitch = dcEc2Snitch.substring(0, dcEc2Snitch.length() - 2);
					_mapDcPubIp.put(dcEc2Snitch, t[1]);
				}
			}

			ip = _mapDcPubIp.get(dc);
			if (ip == null)
				throw new RuntimeException(String.format("No pub ip found for dc %s", dc));
		}
		return ip;
	}

	static public String GetObjLoc(String objId) throws Exception {
		String q = String.format("select obj_id, locations from %s.obj_loc where obj_id='%s'"
				, _ks_obj_loc, objId);
		Statement s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
		try {
			ResultSet rs = _GetSession().execute(s);
			List<Row> rows = rs.all();
			if (rows.size() == 0) {
				return null;
			} else if (rows.size() == 1) {
				Row r = rows.get(0);
				Set<String> locs = r.getSet(1, String.class);
				int locSize = locs.size();
				if (locSize == 0)
					throw new RuntimeException(String.format("Unexpected: no location for object %s", objId));
				int rand = (ThreadLocalRandom.current()).nextInt(locSize);
				int i = 0;
				for (String l: locs) {
					if (i == rand)
						return l;
					i ++;
				}
			} else {
				throw new RuntimeException(String.format("Unexpected: rows.size()=%d", rows.size()));
			}
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		}
		return null;
	}

	static private int _barrier_id = 0;

	// Wait until everyone gets here
	static public long ExecutionBarrier() throws Exception {
		// Wait for 10 secs by default
		return ExecutionBarrier(10000);
	}
	static public long ExecutionBarrier(long timeOut) throws Exception {
		String q = null;
		long lapTime = 0;
		try {
			Cons.Pnnl("Execution barrier");
			long bt = System.currentTimeMillis();

			// Write us-(local_dc)-(exp_id)-(barrier_id) with CL One. CL doesn't matter it
			// will propagate eventually.
			q = String.format("Insert into %s.t0 (barrier_id) values ('%s-%s-%d');" ,
					_ks_exe_barrier, LocalDC(), Conf.ExpID(), _barrier_id);
			Statement s = new SimpleStatement(q);
			_GetSession().execute(s);

			// Keep reading us-(remote_dc)-(exp_id)-(barrier_id) with CL LOCAL_ONE until
			// it sees the message from the other side. This doesn't need to be
			// parallelized.
			for (String peer_dc: _remoteDCs) {
				q = String.format("select barrier_id from %s.t0 where barrier_id='%s-%s-%d';" ,
						_ks_exe_barrier, peer_dc, Conf.ExpID(), _barrier_id);
				s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
				int sleepCnt = 0;
				while (true) {
					ResultSet rs = _GetSession().execute(s);
					List<Row> rows = rs.all();
					if (rows.size() == 0) {
						if (sleepCnt % 10 == 9) {
							if (sleepCnt == 9)
								System.out.printf(" ");
							System.out.printf(".");
							System.out.flush();
						}
						Thread.sleep(10);
						sleepCnt ++;
					} else if (rows.size() == 1) {
						lapTime = System.currentTimeMillis() - bt;
						break;
					} else
						throw new RuntimeException(String.format("Unexpected: rows.size()=%d", rows.size()));

					lapTime = System.currentTimeMillis() - bt;
					if (lapTime > timeOut) {
						System.out.printf("\n");
						throw new RuntimeException("Execution barrier wait timed out :(");
					}
				}
			}

			_barrier_id ++;

			System.out.printf(" took %d ms\n", System.currentTimeMillis() - bt);
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		}
		return lapTime;
	}

	static public void WriteStartTime(long startTime) throws Exception {
		String k = String.format("starttime-%s", Conf.ExpID());
		String v = Long.toString(startTime);

		PreparedStatement ps = _GetSession().prepare(
				String.format("insert into %s.t0 (key, value) values (?,?)", _ks_exp_meta));
		BoundStatement bs = new BoundStatement(ps);
		_GetSession().execute(bs.bind(k, v));
	}

	static public long ReadStartTimeUntilSucceed() throws Exception {
		String k = String.format("starttime-%s", Conf.ExpID());
		String q = String.format("select * from %s.t0 where key='%s'"
				, _ks_exp_meta, k);
		try {
			Statement s = new SimpleStatement(q).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
			Cons.Pnnl("Getting start time:");
			long bt = System.currentTimeMillis();
			int cntSleep = 0;
			while (true) {
				ResultSet rs = _GetSession().execute(s);
				List<Row> rows = rs.all();
				if (rows.size() == 0) {
					if (cntSleep % 10 == 9) {
						if (cntSleep == 9)
							System.out.print(" ");
						System.out.print(".");
						System.out.flush();
					}
					Thread.sleep(10);
					cntSleep ++;
				} else if (rows.size() == 1) {
					Row r = rows.get(0);
					String v = r.getString("value");
					//Cons.P("v=%s", v);
					System.out.printf(" took %d ms\n", System.currentTimeMillis() - bt);
					return Long.parseLong(v);
				} else {
					throw new RuntimeException(String.format("Unexpected: rows.size()=%d", rows.size()));
				}

				if (System.currentTimeMillis() - bt > 2000) {
					System.out.printf("\n");
					throw new RuntimeException("Time out :(");
				}
			}
		} catch (com.datastax.driver.core.exceptions.DriverException e) {
			Cons.P("Exception=[%s] query=[%s]", e, q);
			throw e;
		}
	}
}
