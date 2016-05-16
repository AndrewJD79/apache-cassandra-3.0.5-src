import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.yaml.snakeyaml.Yaml;


class Conf {
	private static String _dt_begin;
	private static final OptionParser _opt_parser = new OptionParser() {{
		accepts("help", "Show this help message");
	}};

	private static void _PrintHelp() throws java.io.IOException {
		System.out.println("Usage: PartialRep [<option>]* dt_begin");
		System.out.println("  dt_begin: begin date time, which identifies the run. Try `date +\"%y%m%d-%H%M%S\"`.\n");
		_opt_parser.printHelpOn(System.out);
	}

	public static void ParseArgs(String[] args)
		throws java.io.IOException, java.text.ParseException, java.lang.InterruptedException {

		OptionSet options = _opt_parser.parse(args);
		if (options.has("help")) {
			_PrintHelp();
			System.exit(0);
		}
		List<?> nonop_args = options.nonOptionArguments();
		if (nonop_args.size() != 1) {
			_PrintHelp();
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

	private static void _LoadYaml() throws IOException {
		Map root = (Map) ((new Yaml()).load(new FileInputStream(new File("/home/ubuntu/work/acorn/conf/cassandra.yaml"))));

		acornOptions = new AcornOptions(root.get("acorn_options"));
		//acornLoadgenOptions = new AcornLoadgenOptions(root.get("acorn_loadgen_options"));
	}

	public static class AcornOptions {
		// Keep underscore notations for the future when parsing is automated
		String keyspace_prefix;
		long attr_pop_broadcast_interval_in_ms;
		long attr_pop_monitor_window_size_in_ms;

		AcornOptions(Object obj) {
			Map o = (Map) obj;
			attr_pop_broadcast_interval_in_ms  = Long.parseLong(o.get("attr_pop_broadcast_interval_in_ms").toString());
			attr_pop_monitor_window_size_in_ms = Long.parseLong(o.get("attr_pop_monitor_window_size_in_ms").toString());
		}

		@Override
		public String toString() {
			return ReflectionToStringBuilder.toString(this);
		}
	}

	//public static class AcornLoadgenOptions {
	//	public final Db db;

	//	AcornLoadgenOptions(Object obj) {
	//		Map o = (Map) obj;
	//		db = new Db((Map) o.get("db"));
	//	}

	//	@Override
	//	public String toString() {
	//		return ReflectionToStringBuilder.toString(this);
	//	}

	//	public class Db {
	//		boolean requests;

	//		Db(Map m) {
	//			requests = Boolean.parseBoolean(m.get("requests").toString());
	//		}

	//		@Override
	//			public String toString() {
	//				return ReflectionToStringBuilder.toString(this);
	//			}
	//	}
	//}

	public static AcornOptions acornOptions = null;
	//public static AcornLoadgenOptions acornLoadgenOptions = null;
}
