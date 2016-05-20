import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Date;

// Progress monitor
class ProgMon {
	private static AtomicInteger _extraSleepRunningOnTimeCnt = new AtomicInteger(0);
	private static AtomicLong _extraSleepRunningOnTimeSum = new AtomicLong(0);
	private static AtomicInteger _extraSleepRunningBehindCnt = new AtomicInteger(0);
	private static AtomicLong _extraSleepRunningBehindSum = new AtomicLong(0);

	private static boolean _stopRequested = false;

	private static class MonThread implements Runnable
	{
		public void run() {
			// Monitor the progress by the number of requested writes / all writes.
			try {
				int w_prev = 0;
				int r_prev = 0;
				int rm_prev = 0;
				String fmt = "%7d %13s %13s"
					+ " %5.1f %6.0f %6.0f"
					+ " %8.3f %8.3f"
					+ " %5d"
					+ " %9d %9d"
					+ " %6d %8d"
					+ " %6d %8d"
					;
				Cons.P(Util.BuildHeader(fmt, 0
							, "simulation_time_dur_ms", "simulation_time", "simulated_time"
							, "percent_completed"
							, "num_w_per_sec", "num_r_per_sec"
							, "write_latency_ms", "read_latency_ms"
							, "read_misses"
							, "eth0_rx", "eth0_tx"
							, "running_on_time_cnt", "running_on_time_sleep_avg_in_ms"
							, "running_behind_cnt", "running_behind_avg_in_ms"
							));
				while (true) {
					synchronized (this) {
						wait(Conf.acornYoutubeOptions.prog_mon_report_interval_in_ms);
					}
					if (_stopRequested)
						break;

					int w = _writeRequested.get();
					int r = _readRequested.get();
					int wr = w + r;
					int rm = _readMisses.get();

					Util.RxTx rt = XDcTrafficMon.Get();

					int extraSleepRunningOnTimeCnt = _extraSleepRunningOnTimeCnt.get();
					long extraSleepRunningOnTimeAvg = (extraSleepRunningOnTimeCnt == 0) ?
						0 : (_extraSleepRunningOnTimeSum.get() / extraSleepRunningOnTimeCnt);
					int extraSleepRunningBehindCnt = _extraSleepRunningBehindCnt.get();
					long extraSleepRunningBehindAvg = (extraSleepRunningBehindCnt == 0) ?
						0 : (_extraSleepRunningBehindSum.get() / extraSleepRunningBehindCnt);
					LatMon.Result latency = LatMon.GetAndReset();

					long curTime = System.currentTimeMillis();
					SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd-HHmmss");
					String simulationTimeStr = sdf.format(new Date(curTime));
					String simulatedTimeStr = sdf.format(new Date(SimTime.ToSimulatedTime(curTime)));

					Cons.P(fmt
								, curTime - SimTime.GetStartSimulationTime(), simulationTimeStr, simulatedTimeStr
								, 100.0 * wr / YoutubeData.NumReqs()
								, 1000.0 * (w - w_prev) / Conf.acornYoutubeOptions.prog_mon_report_interval_in_ms
								, 1000.0 * (r - r_prev) / Conf.acornYoutubeOptions.prog_mon_report_interval_in_ms
								, latency.avgWriteTime / 1000000.0, latency.avgReadTime / 1000000.0
								, rm - rm_prev
								, rt.rx, rt.tx
								, extraSleepRunningOnTimeCnt, extraSleepRunningOnTimeAvg
								, extraSleepRunningBehindCnt, extraSleepRunningBehindAvg
								);
					if (wr == YoutubeData.NumReqs())
						break;

					w_prev = w;
					r_prev = r;
					rm_prev = rm;
					_extraSleepRunningOnTimeCnt.set(0);
					_extraSleepRunningOnTimeSum.set(0);
					_extraSleepRunningBehindCnt.set(0);
					_extraSleepRunningBehindSum.set(0);

					//System.out.flush();
				}

				{
					int w = _writeRequested.get();
					int r = _readRequested.get();
					int wr = w + r;
					int rm = _readMisses.get();

					Util.RxTx rt = XDcTrafficMon.Get();

					int extraSleepRunningOnTimeCnt = _extraSleepRunningOnTimeCnt.get();
					long extraSleepRunningOnTimeAvg = (extraSleepRunningOnTimeCnt == 0) ?
						0 : (_extraSleepRunningOnTimeSum.get() / extraSleepRunningOnTimeCnt);
					int extraSleepRunningBehindCnt = _extraSleepRunningBehindCnt.get();
					long extraSleepRunningBehindAvg = (extraSleepRunningBehindCnt == 0) ?
						0 : (_extraSleepRunningBehindSum.get() / extraSleepRunningBehindCnt);
					LatMon.Result latency = LatMon.GetAndReset();

					long curTime = System.currentTimeMillis();
					SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd-HHmmss");
					String simulationTimeStr = sdf.format(new Date(curTime));
					String simulatedTimeStr = sdf.format(new Date(SimTime.ToSimulatedTime(curTime)));

					Cons.P(fmt
								, curTime - SimTime.GetStartSimulationTime(), simulationTimeStr, simulatedTimeStr
								, 100.0 * wr / YoutubeData.NumReqs()
								, 1000.0 * (w - w_prev) / Conf.acornYoutubeOptions.prog_mon_report_interval_in_ms
								, 1000.0 * (r - r_prev) / Conf.acornYoutubeOptions.prog_mon_report_interval_in_ms
								, latency.avgWriteTime / 1000000.0, latency.avgReadTime / 1000000.0
								, rm - rm_prev
								, rt.rx, rt.tx
								, extraSleepRunningOnTimeCnt, extraSleepRunningOnTimeAvg
								, extraSleepRunningBehindCnt, extraSleepRunningBehindAvg
								);
				}

				// Overall stat
				//   Total # of writes and reads
				//   Read/write latency: min, max, avg, 50, 90, 95, and 99-th percentiles.

				int w = _writeRequested.get();
				int r = _readRequested.get();
				int rm = _readMisses.get();
				Cons.P("#");
				Cons.P("# writes: %d", w);
				Cons.P("# reads : %d", r);
				Cons.P("# reads / write: %f", ((double)r)/w);
				Cons.P("# read misses: %d", rm);

				LatMon.Stat wStat = LatMon.GetWriteStat();
				LatMon.Stat rStat = LatMon.GetReadStat();
				Cons.P("#");
				Cons.P("# Write latency:");
				Cons.P("#   avg=%8.3f min=%8.3f max=%8.3f 50=%8.3f 90=%8.3f 95=%8.3f 99=%8.3f 995=%8.3f 999=%8.3f"
							, wStat.avg  / 1000000.0
							, wStat.min  / 1000000.0
							, wStat.max  / 1000000.0
							, wStat._50  / 1000000.0
							, wStat._90  / 1000000.0
							, wStat._95  / 1000000.0
							, wStat._99  / 1000000.0
							, wStat._995 / 1000000.0
							, wStat._999 / 1000000.0
							);
				Cons.P("# Read latency:");
				Cons.P("#   avg=%8.3f min=%8.3f max=%8.3f 50=%8.3f 90=%8.3f 95=%8.3f 99=%8.3f 995=%8.3f 999=%8.3f"
							, rStat.avg  / 1000000.0
							, rStat.min  / 1000000.0
							, rStat.max  / 1000000.0
							, rStat._50  / 1000000.0
							, rStat._90  / 1000000.0
							, rStat._95  / 1000000.0
							, rStat._99  / 1000000.0
							, rStat._995 / 1000000.0
							, rStat._999 / 1000000.0
							);
			} catch (Exception e) {
				System.out.printf("Exception: %s\n%s\n", e, Util.GetStackTrace(e));
				System.exit(1);
			}
		}
	}

	private static MonThread _mt = new MonThread();
	private static Thread _thrMt = new Thread(_mt);

	public static void Start() throws Exception {
		XDcTrafficMon.Start();
		_thrMt.start();
	}

	public static void Stop() throws Exception {
		_stopRequested = true;
		synchronized (_mt) {
			_mt.notifyAll();
		}
		_thrMt.join();
		XDcTrafficMon.Stop();
	}

	private static AtomicInteger _writeRequested = new AtomicInteger(0);
	private static AtomicInteger _readRequested = new AtomicInteger(0);
	private static AtomicInteger _readMisses = new AtomicInteger(0);

	public static void Write() {
		_writeRequested.incrementAndGet();
	}

	public static void Read() {
		_readRequested.incrementAndGet();
	}

	public static void ReadMiss() {
		_readMisses.incrementAndGet();
	}

	public static void SimulatorRunningOnTime(long extraSleep) {
		_extraSleepRunningOnTimeCnt.incrementAndGet();
		_extraSleepRunningOnTimeSum.addAndGet(extraSleep);
	}

	public static void SimulatorRunningBehind(long extraSleep) {
		_extraSleepRunningBehindCnt.incrementAndGet();
		_extraSleepRunningBehindSum.addAndGet(extraSleep);
	}
}
