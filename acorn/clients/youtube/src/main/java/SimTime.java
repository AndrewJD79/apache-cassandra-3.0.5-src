import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

public class SimTime
{
	private static long startSimulationTimeMs = -1;

	private static long startSimulatedTimeMs = -1;
	private static long endSimulatedTimeMs;
	private static long simulatedTimeDurMs;

	public static void Init(YoutubeData.Req lastReq) throws Exception {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d = df.parse("2010-08-01 00:00:00");
		//Cons.P(d);
		startSimulatedTimeMs = d.getTime();
		//Cons.P(startSimulatedTimeMs);

		// Force calculation of the simulated time in ms
		lastReq.GetSimulatedTime();
		endSimulatedTimeMs = lastReq.simulatedTime;

		simulatedTimeDurMs = endSimulatedTimeMs - startSimulatedTimeMs;
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

	private static long _simulationTimeDur = Conf.acornYoutubeOptions.simulation_time_dur_in_ms;

	public static long ToSimulationTime(long simulatedTime) {
		// simulationTime - startSimulationTimeMs : _simulationTimeDur
		// 	= simulatedTime - startSimulatedTimeMs : simulatedTimeDurMs
		//
		// simulationTime =
		// (_simulationTimeDur * (simulatedTime - startSimulatedTimeMs)) / simulatedTimeDurMs + startSimulationTimeMs

		return (long) (((double) _simulationTimeDur * (simulatedTime - startSimulatedTimeMs)) / simulatedTimeDurMs + startSimulationTimeMs);
	}

	public static long ToSimulatedTime(long simulationTime) {
		// simulationTime - startSimulationTimeMs : _simulationTimeDur
		// 	= simulatedTime - startSimulatedTimeMs : simulatedTimeDurMs
		//
		// simulatedTime = (simulationTime - startSimulationTimeMs) * simulatedTimeDurMs / _simulationTimeDur + startSimulatedTimeMs
		return (long) (((double) simulationTime - startSimulationTimeMs) * simulatedTimeDurMs / _simulationTimeDur + startSimulatedTimeMs);
	}

	public static long ToSimulatedTimeInterval(long simulationTimeInterval) {
		// simulationTimeInterval : _simulationTimeDur
		// 	= simulatedTimeInterval : simulatedTimeDurMs
		return (long) ((double) simulationTimeInterval * simulatedTimeDurMs / _simulationTimeDur);
	}
}
