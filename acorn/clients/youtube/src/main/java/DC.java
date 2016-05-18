import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class DC {
	static List<String> allDCs = new ArrayList<String>();
	static String localAz = null;
	static String localDc = null;
	static List<String> remoteDCs = new ArrayList<String>();

	static public void Init() throws Exception {
		// Get local and remote DC names from .run/dc-ip-map
		File fn_jar = new File(AcornYoutube.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
		String fn = String.format("%s/.run/dc-ip-map", fn_jar.getParentFile().getParentFile());
		try (BufferedReader br = new BufferedReader(new FileReader(fn))) {
			String line;
			while ((line = br.readLine()) != null) {
				// us-east-1 54.160.118.182
				String[] t = line.split("\\s+");
				if (t.length !=2)
					throw new RuntimeException(String.format("Unexpcted format [%s]", line));
				allDCs.add(t[0]);
			}
		}
		Cons.P("allDCs: %s", String.join(", ", allDCs));

		// Get local DC and AZ names.
		Runtime r = Runtime.getRuntime();
		Process p = r.exec("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone");
		p.waitFor();
		BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line = "";
		while ((line = b.readLine()) != null) {
			localAz = line;
		}
		b.close();
		// Trim the last [a-z].
		localDc = localAz.substring(0, localAz.length() - 1);
		Cons.P("Local DC=%s AZ=%s", localDc, localAz);



		System.exit(0);

		// TODO
//		// Calc remote DC names.
//		for (String: dc: allDCs) {
//			if (dc.star
//		}

	}

	static public boolean IsLocalDcTheClosestToReq(YoutubeData.Req r) {
		// TODO
		return true;
//		if (localAz == null) {
//		}
//
//		// Get remote DC names from .run/dc-ip-map
//
//
//		for (Map.Entry<String, Coord> e: Conf.acornYoutubeOptions.mapDcCoord.entrySet()) {
//			e.getKey()
//				e.getValue()
//		}
	}

	public static class Coord {
		double longi;
		double lati;

		Coord(double longi, double lati) {
			this.longi = longi;
			this.lati = lati;
		}

		@Override
		public String toString() {
			return ReflectionToStringBuilder.toString(this);
		}
	}
}
