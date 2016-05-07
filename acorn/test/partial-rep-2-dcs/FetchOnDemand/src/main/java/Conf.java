import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Arrays;

import joptsimple.OptionParser;
import joptsimple.OptionSet;


class Conf {
	public static String dt_begin;
	public static final String[] hns = {"mdc-s40", "mdc-s50"};
	public static String data_dir = "/mnt/mdc-data/pr/2n/test";
	public static String dc;
	private static final OptionParser _opt_parser = new OptionParser() {{
		accepts("help", "Show this help message");
	}};

	private static void _PrintHelp() throws java.io.IOException {
		System.out.println("Usage: FetchOnDemand [<option>]* dt_begin");
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

		dt_begin = (String) nonop_args.get(0);
		SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd-HHmmss");
		long dt_begin_milli = sdf.parse(dt_begin).getTime();

		int i = 0;
		for (; i < hns.length; i ++) {
			if (Util.Hostname().equals(hns[i])) {
				dc = String.format("DC%d", i);
				break;
			}
		}
		if (i == hns.length)
			throw new RuntimeException("unknown hostname: " + Util.Hostname());

		System.out.printf("dt_begin: %s %d\n", dt_begin, dt_begin_milli);
		System.out.printf("hns:      %s\n", Arrays.toString(hns));
		System.out.printf("data_dir: %s\n", data_dir);
		System.out.printf("dc:       %s\n", dc);
	}


	public static int NumDCs() {
		return hns.length;
	}


	private static String _cass_src_dn = null;
	public static String CassandraSrcDn() {
		if (_cass_src_dn != null)
			return _cass_src_dn;
		_cass_src_dn = System.getProperty("user.home") + "/work/cassandra";
		return _cass_src_dn;
	}
}
