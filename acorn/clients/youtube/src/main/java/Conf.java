import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.yaml.snakeyaml.Yaml;


class Conf {
	private static String _dt_begin;
	private static final OptionParser _opt_parser = new OptionParser() {{
		accepts("help", "Show this help message");
	}};

	private static void _PrintHelp(String[] args) throws java.io.IOException {
		System.out.printf("Usage: %s [<option>]* dt_begin\n", args[0]);
		System.out.printf("  dt_begin: begin date time, which identifies the run. Try `date +\"%y%m%d-%H%M%S\"`.\n");
		_opt_parser.printHelpOn(System.out);
	}

	public static void Init(String[] args) throws Exception {
		OptionSet options = _opt_parser.parse(args);
		if (options.has("help")) {
			_PrintHelp(args);
			System.exit(0);
		}
		List<?> nonop_args = options.nonOptionArguments();
		if (nonop_args.size() != 1) {
			_PrintHelp(args);
			System.exit(1);
		}

		// I don't think I need a hostname. I only need a DC name. Interesting that
		// Cassandra thinks it's just us-east and us-west, not like us-east-1. I
		// wonder what's gonna happen when you add both us-west-1 and us-west-2. If
		// they change dynamically, wouldn't it make any trouble?
		//
		// $ nodetool status
		// Datacenter: us-east
		// ===================
		// Status=Up/Down
		// |/ State=Normal/Leaving/Joining/Moving
		// --  Address         Load       Tokens       Owns (effective)  Host ID                               Rack
		// UN  54.160.83.23    211.5 KB   256          100.0%            ce88ef0e-0bca-458d-ba95-02e8cf755642  1e
		// Datacenter: us-west
		// ===================
		// Status=Up/Down
		// |/ State=Normal/Leaving/Joining/Moving
		// --  Address         Load       Tokens       Owns (effective)  Host ID                               Rack
		// UN  54.177.212.255  211.71 KB  256          100.0%            3c7a698c-705c-42c4-b27e-93262c53b7b3  1b
		//
		// http://stackoverflow.com/questions/19489498/getting-cassandra-datacenter-name-in-cqlsh
		//
		//Cons.P("hostname: %s", Util.Hostname());

		_dt_begin = (String) nonop_args.get(0);

		_LoadYaml();
	}

	public static String ExpID() {
		return _dt_begin;
	}

	private static void _LoadYaml() throws Exception {
		File fn_jar = new File(AcornYoutube.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
		// /mnt/local-ssd0/work/apache-cassandra-3.0.5-src/acorn/clients/youtube/target/AcornYoutube-0.1.jar

		{
			String fn = String.format("%s/conf/cassandra.yaml", fn_jar.getParentFile().getParentFile().getParentFile().getParentFile().getParentFile());
			Map root = (Map) ((new Yaml()).load(new FileInputStream(new File(fn))));
			Object o = root.get("acorn_options");
			if (! (o instanceof Map))
				throw new RuntimeException(String.format("Unexpected: o.getClass()=%s", o.getClass().getName()));
			acornOptions = new AcornOptions((Map) o);
			Cons.P(acornOptions);
		}

		{
			String fn = String.format("%s/acorn-youtube.yaml", fn_jar.getParentFile().getParentFile());
			Object root = (new Yaml()).load(new FileInputStream(new File(fn)));
			if (! (root instanceof Map))
				throw new RuntimeException(String.format("Unexpected: root.getClass()=%s", root.getClass().getName()));
			acornYoutubeOptions = new AcornYoutubeOptions((Map) root);
			Cons.P(acornYoutubeOptions);
		}
	}

	public static class AcornOptions {
		// Keep underscore notations for the future when the parsing is automated,
		// which I would love to in the future.
		long attr_pop_broadcast_interval_in_ms;
		long attr_pop_monitor_window_size_in_ms;

		AcornOptions(Map m) {
			attr_pop_broadcast_interval_in_ms  = Long.parseLong(m.get("attr_pop_broadcast_interval_in_ms").toString());
			attr_pop_monitor_window_size_in_ms = Long.parseLong(m.get("attr_pop_monitor_window_size_in_ms").toString());
		}

		@Override
		public String toString() {
			return ReflectionToStringBuilder.toString(this);
		}
	}

	public static class AcornYoutubeOptions {
		public String dn_data;
		public String fn_users;
		public String fn_youtube_reqs;
		public Map<String, DC.Coord> mapDcCoord = new TreeMap<String, DC.Coord>();
		public long simulation_time_dur_in_ms;
		public int num_threads;
		public int youtube_extra_data_size;
		public long read_req_delay_in_simulation_time_in_ms;
		public long max_requests;
		public long prog_mon_report_interval_in_ms;

		AcornYoutubeOptions(Map m) {
			dn_data = (String) m.get("dn_data");
			fn_users = (String) m.get("fn_users");
			fn_youtube_reqs = (String) m.get("fn_youtube_reqs");
			fn_youtube_reqs = (String) m.get("fn_youtube_reqs");
			for (Map.Entry<String, String> e: ((Map<String, String>) m.get("dc_coordinates")).entrySet()) {
				String k = e.getKey();
				String v = e.getValue();
				//Cons.P("[%s] [%s]", k, v);

				String[] t = v.split(", ");
				if (t.length != 2)
					throw new RuntimeException(String.format("Unexpected: t.length=%d", t.length));
				double longi = Double.parseDouble(t[0]);
				double lati = Double.parseDouble(t[1]);
				mapDcCoord.put(k, new DC.Coord(longi, lati));
			}
			simulation_time_dur_in_ms = (int) m.get("simulation_time_dur_in_ms");
			num_threads = (int) m.get("num_threads");
			youtube_extra_data_size = (int) m.get("youtube_extra_data_size");
			read_req_delay_in_simulation_time_in_ms = (int) m.get("read_req_delay_in_simulation_time_in_ms");
			max_requests = (int) m.get("max_requests");
			prog_mon_report_interval_in_ms = (int) m.get("prog_mon_report_interval_in_ms");
		}

		@Override
		public String toString() {
			return ReflectionToStringBuilder.toString(this);
		}
	}

	public static AcornOptions acornOptions = null;
	public static AcornYoutubeOptions acornYoutubeOptions = null;
}
