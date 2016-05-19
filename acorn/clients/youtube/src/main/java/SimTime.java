import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class SimTime
{
	private static long startSimulationTimeMs = -1;

	// https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html
	private static SimpleDateFormat _sdf = new SimpleDateFormat("yyMMdd-HHmmss.SSS");

	static {
		_sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
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

	public static void SleepUntilSimulatedTime(YoutubeData.Req r) {
		// TODO: implement
	}

	public static String GetSimulationTimeStr(long simulationTime) {
		Date d = new Date(simulationTime);
		return _sdf.format(d);
	}

	public static String GetSimulatedTimeStr(long simulationTime) {
		// TODO: implement
		return "aa-bb-cc";
	}
}



//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//
//import org.apache.commons.lang3.time.DurationFormatUtils;
//
//class SimTime {
//	// Simulation time is the wall-clock time in which this simulator is running.
//	// Simulated time is the time that this simulator simulates, e.g., from
//	// 2010-01-01 to 2015-01-01.
//	private static LocalDateTime _simulatedTimeBegin;
//	private static LocalDateTime _simulatedTimeEnd;
//	// In seconds from Epoch
//	private static long _simulatedTimeBeginEs;
//	private static long _simulatedTimeEndEs;
//	private static long _simulatedTimeDurEs;
//
//	private static ZoneId _zoneId;
//
//	// Simulation time in nano seconds. These are relative numbers.
//	private static long _simulationTimeBeginNano;
//	private static long _simulationTimeEndNano;
//	private static long _simulationTimeDurNano;
//	// This is an absolute number, which can be used to build a Date() object.
//	private static long _simulationTimeBeginMilli;
//
//	public static void Init() {
//		_simulatedTimeBegin = LocalDateTime.of(2010, 1, 1, 0, 0);
//		_simulatedTimeEnd = _simulatedTimeBegin.plusSeconds((long)(Conf.mutantsOptions.simulated_time_years * 365.25 * 24 * 3600));
//
//		_zoneId = ZoneId.systemDefault();
//
//		// Convert to Epoch seconds. Ignore sub-second resolution
//		_simulatedTimeBeginEs = _simulatedTimeBegin.atZone(_zoneId).toEpochSecond();
//		_simulatedTimeEndEs = _simulatedTimeEnd.atZone(_zoneId).toEpochSecond();
//		_simulatedTimeDurEs = _simulatedTimeEndEs - _simulatedTimeBeginEs;
//
//		Cons.P(String.format("Simulated datetime:"
//					+ "\n  begin: %16s %10d"
//					+ "\n  end:   %16s %10d"
//					//+ "\n  dur:   %16s %10d"
//					, _simulatedTimeBegin, _simulatedTimeBeginEs
//					, _simulatedTimeEnd, _simulatedTimeEndEs
//					//, DurationFormatUtils.formatDurationHMS(_simulatedTimeDurEs * 1000), _simulatedTimeDurEs
//					));
//	}
//
//	public static long SimulatedTimeBeginEs() {
//		return _simulatedTimeBeginEs;
//	}
//
//	public static long SimulatedTimeEndEs() {
//		return _simulatedTimeEndEs;
//	}
//
//	public static long SimulatedTimeDurEs() {
//		return _simulatedTimeDurEs;
//	}
//
//	public static long SimulationTimeBeginNano() {
//		return _simulationTimeBeginNano;
//	}
//
//	public static void StartSimulation() {
//		_simulationTimeDurNano = (long) (Conf.mutantsOptions.simulation_time_mins * 60 * 1000000000);
//		_simulationTimeBeginNano = System.nanoTime();
//		_simulationTimeBeginMilli = System.currentTimeMillis();
//		_simulationTimeEndNano = _simulationTimeBeginNano + _simulationTimeDurNano;
//		Cons.P(String.format("Simulation time:"
//					+ "\n  begin: %16d"
//					+ "\n  end:   %16d"
//					+ "\n  dur:   %16d"
//					, _simulationTimeBeginNano
//					, _simulationTimeEndNano
//					, _simulationTimeDurNano
//					));
//	}
//
//	public static String GetCurSimulatedTime() {
//		return GetSimulatedTime(System.nanoTime());
//	}
//
//
//	// yymmdd-HHMMSS.SSS
//	// 01234567890123456
//	public static String GetSimulatedTime(long curTimeInNs) {
//		// curSimulationTime - _simulationTimeBeginNano : _simulationTimeDurNano
//		// 	= curSimulatedTime - _simulatedTimeBegin : _simulatedTimeDur
//		//
//		// curSimulatedTime = (curSimulationTime - _simulationTimeBeginNano)
//		//   * _simulatedTimeDur / _simulationTimeDurNano + _simulatedTimeBegin
//
//		// In epoch sec
//		double curSimulatedTimeEs = (double) (curTimeInNs - _simulationTimeBeginNano)
//			* _simulatedTimeDurEs / _simulationTimeDurNano + _simulatedTimeBeginEs;
//
//		Date d0 = new Date((long)(curSimulatedTimeEs * 1000));
//		return _sdf.format(d0);
//	}
//
//	public static String GetSimulationTime(long curTimeInNs) {
//		Date d0 = new Date(_simulationTimeBeginMilli + (curTimeInNs - _simulationTimeBeginNano) / 1000000);
//		return _sdf.format(d0);
//	}
//
//	// Average Thread.sleep(0, 1) time is 1.1 ms on a 8 core Xeon E5420 @ 2.50GHz
//	// machine
//	final private static long _minSleepNano = 2000000;
//
//	public static void SleepUntilSimulatedTime(long simulatedTimeEs) throws InterruptedException {
//		long durSinceSimulatedTimeBegin = simulatedTimeEs - _simulatedTimeBeginEs;
//
//		// _simulationTimeDurNano : _simulatedTimeDurEs
//		//   = targetDurSinceSimulationTimeBegin : durSinceSimulatedTimeBegin
//		//
//		// targetDurSinceSimulationTimeBegin = (targetSimulationTime - _simulationTimeBeginNano)
//		//
//		// sleep for (targetSimulationTime - curTime)
//
//		//targetDurSinceSimulationTimeBegin = _simulationTimeDurNano * durSinceSimulatedTimeBegin / _simulatedTimeDurEs;
//		//(targetSimulationTime - _simulationTimeBeginNano) = _simulationTimeDurNano * durSinceSimulatedTimeBegin / _simulatedTimeDurEs;
//		long targetSimulationTime = (long) (((double) _simulationTimeDurNano) * durSinceSimulatedTimeBegin
//				/ _simulatedTimeDurEs + _simulationTimeBeginNano);
//		long curTime = System.nanoTime();
//		long extraSleep = targetSimulationTime - curTime;
//		//Cons.P(String.format("extraSleep: %10d %4d %7d"
//		//			, extraSleep
//		//			, extraSleep / 1000000
//		//			, extraSleep % 1000000
//		//			));
//		if (extraSleep > 0) {
//			ProgMon.SimulatorRunningOnTime(extraSleep);
//		} else {
//			ProgMon.SimulatorRunningBehind(extraSleep);
//		}
//		if (extraSleep <= _minSleepNano)
//			return;
//		Thread.sleep(extraSleep / 1000000, (int) (extraSleep % 1000000));
//	}
//
//	public static void TestMinimumSleeptime() throws InterruptedException {
//		try (Cons.MeasureTime _ = new Cons.MeasureTime("Testing minimum Thread.sleep() time ...")) {
//			int runs = 1000;
//			List<Long> sleepTimes = new ArrayList();
//			for (int i = 0; i < runs; i ++) {
//				long before = System.nanoTime();
//				Thread.sleep(0, 1);
//				long after = System.nanoTime();
//				sleepTimes.add(after - before);
//			}
//
//			long min = -1;
//			long max = -1;
//			long sum = 0;
//			boolean first = true;
//			for (long st: sleepTimes) {
//				if (first) {
//					min = max = st;
//					first = false;
//				} else {
//					if (st < min) {
//						min = st;
//					} else if (max < st) {
//						max = st;
//					}
//				}
//				sum += st;
//			}
//			double avg = ((double) sum) / runs;
//
//			Cons.P(String.format(
//						"In nano seconds"
//						+ "\navg: %f"
//						+ "\nmin: %d"
//						+ "\nmax: %d"
//						+ "\nruns: %d"
//						, avg
//						, min
//						, max
//						, runs
//						));
//		}
//	}
//}
