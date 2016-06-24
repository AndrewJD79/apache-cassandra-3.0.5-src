import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class DC {
	// Need to be ordered to get the same ordering from all AWS datacenters when
	// calculating the cloests DC from a Req.
	private static Set<String> allDCsAvailable = new TreeSet<String>();

	private static String localAz = null;
	private static String localDc = null;
	static List<String> remoteDCs = new ArrayList<String>();

	static public void Init() throws Exception {
		try (Cons.MT _ = new Cons.MT("DC.Init() ...")) {
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
					allDCsAvailable.add(t[0]);
				}
			}
			Cons.P("allDCsAvailable: %s", String.join(", ", allDCsAvailable));

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

			// Calc remote DC names.
			remoteDCs = allDCsAvailable.stream().filter(dc -> (!dc.equals(localDc))).collect(Collectors.toList());
			Cons.P("remoteDCs: %s", String.join(", ", remoteDCs));

			// Cassandra Ec2Snitch sometimes seems to have shorter names, which you
			// could get around with prefix matches.
		}
	}

	static public boolean IsLocalDcTheClosestToReq(YoutubeData.Req r) {
		return GetClosestDcToReq(r).equals(localDc);
	}

	// This won't break the tie.
	//
	//private static Coord localDcCoord = null;
	//
	//static public boolean IsLocalDcTheClosestToReq(YoutubeData.Req r) {
	//	if (localDcCoord == null)
	//		localDcCoord = Conf.acornYoutubeOptions.mapDcCoord.get(localDc);
	//
	//	double localDcToReq = ArcInRadians(localDcCoord, r);
	//
	//	for (String rDc: remoteDcs) {
	//		Coord c = Conf.acornYoutubeOptions.mapDcCoord.get(rDc);
	//		if (c == null)
	//			throw new RuntimeException(String.format("Unexpected: rDc=%s", rDc));
	//
	//		double dcToReq = ArcInRadians(c, r);
	//		// In the case of tie, we won't see the request from any DC. Need to have
	//		// an order to break ties.
	//		if (localDcToReq > dcToReq)
	//			return true;
	//	}
	//	return false;
	//}

	private static Set<String> allDCs = new TreeSet<String>(Arrays.asList(
				"us-east-1"
				, "eu-west-1"
				, "us-west-1"
				, "us-west-2"
				, "eu-central-1"
				, "sa-east-1"
				, "ap-southeast-1"
				, "ap-southeast-2"
				, "ap-northeast-1"
				));

	static public String GetClosestDcToReq(YoutubeData.Req r) {
		String closestDc = null;
		// Initial value doesn't matter. Just to make the compiler quiet.
		double shortestDist = 0;

		Set<String> dcs = null;
		if (Conf.acornYoutubeOptions.use_all_dcs_for_finding_the_local_dc_of_a_req) {
			dcs = allDCs;
		} else {
			dcs = allDCsAvailable;
		}

		for (String dc: dcs) {
			Coord c = Conf.acornYoutubeOptions.mapDcCoord.get(dc);
			if (c == null)
				throw new RuntimeException(String.format("Unexpected: dc=%s", dc));

			double dcToReq = ArcInRadians(c, r);
			if (closestDc == null) {
				closestDc = dc;
				shortestDist = dcToReq;
				continue;
			}
			if (dcToReq < shortestDist) {
				closestDc = dc;
				shortestDist = dcToReq;
			}
		}
		return closestDc;
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

	// http://en.wikipedia.org/wiki/Haversine_formula
	// http://blog.julien.cayzac.name/2008/10/arc-and-distance-between-two-points-on.html

	/** @brief Computes the arc, in radian, between two WGS-84 positions.
	 *
	 * The result is equal to <code>Distance(from,to)/EARTH_RADIUS_IN_METERS</code>
	 *    <code>= 2*asin(sqrt(h(d/EARTH_RADIUS_IN_METERS )))</code>
	 *
	 * where:<ul>
	 *    <li>d is the distance in meters between 'from' and 'to' positions.</li>
	 *    <li>h is the haversine function: <code>h(x)=sin²(x/2)</code></li>
	 * </ul>
	 *
	 * The haversine formula gives:
	 *    <code>h(d/R) = h(from.lat-to.lat)+h(from.lon-to.lon)+cos(from.lat)*cos(to.lat)</code>
	 *
	 * @sa http://en.wikipedia.org/wiki/Law_of_haversines
	 */
	private static double ArcInRadians(Coord c, YoutubeData.Req r)
	{
		return ArcInRadians(c.lati, c.longi, r.geoLati, r.geoLongi);
	}
	private static double ArcInRadians(
			double lat0, double lon0,
			double lat1, double lon1)
	{
		/// @brief The usual PI/180 constant
		final double DEG_TO_RAD = 0.017453292519943295769236907684886;
		/// @brief Earth's quatratic mean radius for WGS-84
		//static const double EARTH_RADIUS_IN_METERS = 6372797.560856;

		double latitudeArc  = (lat0 - lat1) * DEG_TO_RAD;
		double longitudeArc = (lon0 - lon1) * DEG_TO_RAD;
		double latitudeH = Math.sin(latitudeArc * 0.5);
		latitudeH *= latitudeH;
		double lontitudeH = Math.sin(longitudeArc * 0.5);
		lontitudeH *= lontitudeH;
		double tmp = Math.cos(lat0 * DEG_TO_RAD) * Math.cos(lat1 * DEG_TO_RAD);
		return 2.0 * Math.asin(Math.sqrt(latitudeH + tmp*lontitudeH));
	}
}
