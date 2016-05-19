import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

public class SimTime
{
	private static long startSimulationTimeMs = -1;
	private static long startSimulatedTimeMs = -1;

	static {
		try {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date d = df.parse("2010-08-01 00:00:00");
			//Cons.P(d);
			startSimulatedTimeMs = d.getTime();
			//Cons.P(startSimulatedTimeMs);
		} catch (Exception e) {
			System.out.printf("Exception: %s\n%s\n", e, Util.GetStackTrace(e));
			System.exit(1);
		}
	}

	public static void SetStartSimulationTime(long startSimulationTime_) {
		startSimulationTimeMs = startSimulationTime_;

		long timeToStartTime = startSimulationTimeMs - System.currentTimeMillis();
		if (timeToStartTime > 0)
			Cons.P("startSimulationTimeMs %s is %d ms in the future", startSimulationTimeMs, timeToStartTime);
		else
			throw new RuntimeException(String.format("Unexpected: timeToStartTime=%d", timeToStartTime));
	}

	public static long GetStartSimulationTime() {
		return startSimulationTimeMs;
	}

	public static void SleepUntilSimulatedTime(YoutubeData.Req r) throws Exception {
		long simulatedTime = r.GetSimulatedTime();
		long simulationTime = ToSimulationTime(simulatedTime);
		long diff = simulationTime - System.currentTimeMillis();
		//{
		//	SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd-HHmmss");
		//	sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		//	Date d0 = new Date(simulatedTime);
		//	String s0 = sdf.format(d0);
		//	Date d1 = new Date(simulationTime);
		//	String s1 = sdf.format(d1);
		//	Cons.P("simulatedTime=%d (%s) simulationTime=%d (%s) diff=%d",
		//			simulatedTime, s0, simulationTime, s1, diff);
		//}
		if (diff > 0) {
			Thread.sleep(diff);
			ProgMon.SimulatorRunningOnTime(diff);
		} else {
			ProgMon.SimulatorRunningBehind(diff);
		}
	}

	// TODO: make these configurable
	private static long _simulationTimeDur = 8000;
	//private static double _simulatedTimeDur = 5 * 365.25 * 24 * 3600 * 1000;
	//
	// TODO: for testing. Limit the number of request and shorted the simulated time.
	private static double _simulatedTimeDur = 563.5669213 * 24 * 3600 * 1000;

	public static long ToSimulationTime(long simulatedTime) {
		// simulationTime - startSimulationTimeMs : _simulationTimeDur
		// 	= simulatedTime - startSimulatedTimeMs : _simulatedTimeDur
		//
		// simulationTime =
		// (_simulationTimeDur * (simulatedTime - startSimulatedTimeMs)) /_simulatedTimeDur + startSimulationTimeMs

		return (long) (((double) _simulationTimeDur * (simulatedTime - startSimulatedTimeMs)) /_simulatedTimeDur + startSimulationTimeMs);
	}

	public static long ToSimulatedTime(long simulationTime) {
		// simulationTime - startSimulationTimeMs : _simulationTimeDur
		// 	= simulatedTime - startSimulatedTimeMs : _simulatedTimeDur
		//
		// simulatedTime = (simulationTime - startSimulationTimeMs) * _simulatedTimeDur / _simulationTimeDur + startSimulatedTimeMs
		return (long) (((double) simulationTime - startSimulationTimeMs) * _simulatedTimeDur / _simulationTimeDur + startSimulatedTimeMs);
	}

	public static long ToSimulatedTimeInterval(long simulationTimeInterval) {
		// simulationTimeInterval : _simulationTimeDur
		// 	= simulatedTimeInterval : _simulatedTimeDur
		return (long) ((double) simulationTimeInterval * _simulatedTimeDur / _simulationTimeDur);
	}
}
