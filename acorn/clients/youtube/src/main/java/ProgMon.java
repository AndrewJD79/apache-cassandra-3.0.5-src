import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
				int wr_prev = 0;
				String fmt = "%7d %17s %17s"
					+ " %7d %7d %7d"
					+ " %5.1f %6.0f"
					+ " %5d %8d %5d %8d %4d %4d %4d %4d";
				Cons.P(Util.BuildHeader(fmt, 0
							, "simulation_time_dur_ms", "simulation_time", "simulated_time"
							, "num_wr_requested", "num_w_requested", "num_r_requested"
							, "percent_completed", "req_per_sec"
							, "running_on_time_cnt"
							, "running_on_time_sleep_avg_in_ms"
							, "running_behind_cnt"
							, "running_behind_avg_in_ms"
							, "write_latency_ms"
							, "read_latency_ms"
							, "write_cnt"
							, "read_cnt"
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

					int extraSleepRunningOnTimeCnt = _extraSleepRunningOnTimeCnt.get();
					long extraSleepRunningOnTimeAvg = (extraSleepRunningOnTimeCnt == 0) ?
						0 : (_extraSleepRunningOnTimeSum.get() / extraSleepRunningOnTimeCnt / 1000000);
					int extraSleepRunningBehindCnt = _extraSleepRunningBehindCnt.get();
					long extraSleepRunningBehindAvg = (extraSleepRunningBehindCnt == 0) ?
						0 : (_extraSleepRunningBehindSum.get() / extraSleepRunningBehindCnt / 1000000);
					LatMon.Result latency = LatMon.GetAndReset();

					long curTime = System.currentTimeMillis();
					Cons.P(fmt
								, curTime - SimTime.GetStartSimulationTime()
								, SimTime.GetSimulationTimeStr(curTime)
								, SimTime.GetSimulatedTimeStr(curTime)

								, wr, w, r
								, 100.0 * wr / YoutubeData.NumReqs()
								, 1000.0 * (wr - wr_prev) / Conf.acornYoutubeOptions.prog_mon_report_interval_in_ms

								, extraSleepRunningOnTimeCnt
								, extraSleepRunningOnTimeAvg
								, extraSleepRunningBehindCnt
								, extraSleepRunningBehindAvg
								, latency.avgWriteTime / 1000000
								, latency.avgReadTime / 1000000
								, latency.writeCnt
								, latency.readCnt
								);
					if (wr == YoutubeData.NumReqs())
						break;

					wr_prev = w;
					_extraSleepRunningOnTimeCnt.set(0);
					_extraSleepRunningOnTimeSum.set(0);
					_extraSleepRunningBehindCnt.set(0);
					_extraSleepRunningBehindSum.set(0);

					//System.out.flush();
				}

				// Overall stat
				//   Total # of writes and reads
				//   Read/write latency: min, max, avg, 50, 90, 95, and 99-th percentiles.

				int w = _writeRequested.get();
				int r = _readRequested.get();
				Cons.P("#");
				Cons.P("# # of writes: %d", w);
				Cons.P("# # of reads : %d", r);
				Cons.P("# # reads / write: %f", ((double)r)/w);

				LatMon.Stat wStat = LatMon.GetWriteStat();
				LatMon.Stat rStat = LatMon.GetReadStat();
				Cons.P("#");
				Cons.P("# Write latency:");
				Cons.P("#   avg=%6.3f min=%5.3f max=%4d 50=%4d 90=%4d 95=%4d 99=%4d 995=%4d 999=%4d"
							, wStat.avg  / 1000000.0
							, wStat.min  / 1000000.0
							, wStat.max  / 1000000
							, wStat._50  / 1000000
							, wStat._90  / 1000000
							, wStat._95  / 1000000
							, wStat._99  / 1000000
							, wStat._995 / 1000000
							, wStat._999 / 1000000
							);
				Cons.P("# Read latency:");
				Cons.P("#   avg=%6.3f min=%5.3f max=%4d 50=%4d 90=%4d 95=%4d 99=%4d 995=%4d 999=%4d"
							, rStat.avg  / 1000000.0
							, rStat.min  / 1000000.0
							, rStat.max  / 1000000
							, rStat._50  / 1000000
							, rStat._90  / 1000000
							, rStat._95  / 1000000
							, rStat._99  / 1000000
							, rStat._995 / 1000000
							, rStat._999 / 1000000
							);
			} catch (Exception e) {
				System.out.printf("Exception: %s\n%s\n", e, Util.GetStackTrace(e));
				System.exit(1);
			}
		}
	}

	private static MonThread _mt = new MonThread();
	private static Thread _thrMt = new Thread(_mt);

	public static void Start() {
		_thrMt.start();
	}

	public static void Stop() throws InterruptedException {
		_stopRequested = true;
		synchronized (_mt) {
			_mt.notifyAll();
		}
		_thrMt.join();
	}

	private static AtomicInteger _writeRequested = new AtomicInteger(0);
	private static AtomicInteger _readRequested = new AtomicInteger(0);

	public static void Write() {
		_writeRequested.incrementAndGet();
	}

	public static void Read() {
		_readRequested.incrementAndGet();
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
