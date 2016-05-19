import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

// Latency (response time. time to get a whole record, not the first byte.)
// monitor

class LatMon {
	private static List<Long> _writeTimes = new ArrayList();
	private static List<Long> _readTimes = new ArrayList();
	private static List<Long> _writeTimesAll = new ArrayList();
	private static List<Long> _readTimesAll = new ArrayList();

	public static void Write(long time) {
		synchronized (_writeTimes) {
			_writeTimes.add(time);
		}
	}

	public static void Read(long time) {
		synchronized (_readTimes) {
			_readTimes.add(time);
		}
	}

	public static class Result {
		long avgWriteTime;
		long avgReadTime;
		long writeCnt;
		long readCnt;
	}

	public static Result GetAndReset() {
		Result res = new Result();

		{
			long sum = 0;
			long cnt = 0;
			synchronized (_writeTimes) {
				for (long w: _writeTimes) {
					sum += w;
					cnt ++;
					_writeTimesAll.add(w);
				}
				_writeTimes.clear();
			}
			res.writeCnt = cnt;
			res.avgWriteTime = (cnt == 0) ? 0 : (sum / cnt);
		}
		{
			long sum = 0;
			long cnt = 0;
			synchronized (_readTimes) {
				for (long r: _readTimes) {
					sum += r;
					cnt ++;
					_readTimesAll.add(r);
				}
				_readTimes.clear();
			}
			res.readCnt = cnt;
			res.avgReadTime = (cnt == 0) ? 0 : (sum / cnt);
		}

		return res;
	}

	public static class Stat {
		long cnt = -1;
		long min = -1;
		long max = -1;
		long avg = -1;
		long _50 = -1;
		long _90 = -1;
		long _95 = -1;
		long _99 = -1;
		long _995 = -1;
		long _999 = -1;

		// Note: values are sorted
		Stat(List<Long> values) {
			if (values.size() == 0)
				return;

			Collections.sort(values);
			min = values.get(0);
			max = values.get(values.size() - 1);

			boolean set_50 = false;
			boolean set_90 = false;
			boolean set_95 = false;
			boolean set_99 = false;
			boolean set_995 = false;
			boolean set_999 = false;
			long sum = 0;
			cnt = 0;
			int v_size = values.size();
			for (int i = 0; i < v_size; i ++) {
				if ((set_50 == false) && (i >= 0.5 * v_size)) {
					_50 = values.get(i);
					set_50 = true;
				}
				if ((set_90 == false) && (i >= 0.9 * v_size)) {
					_90 = values.get(i);
					set_90 = true;
				}
				if ((set_95 == false) && (i >= 0.95 * v_size)) {
					_95 = values.get(i);
					set_95 = true;
				}
				if ((set_99 == false) && (i >= 0.99 * v_size)) {
					_99 = values.get(i);
					set_99 = true;
				}
				if ((set_995 == false) && (i >= 0.995 * v_size)) {
					_995 = values.get(i);
					set_995 = true;
				}
				if ((set_999 == false) && (i >= 0.999 * v_size)) {
					_999 = values.get(i);
					set_999 = true;
				}
				sum += values.get(i);
				cnt ++;
			}
			avg = (cnt == 0) ? 0 : (sum / cnt);
		}
	}

	public static Stat GetWriteStat() {
		Stat stat = new Stat(_writeTimesAll);
		return stat;
	}

	public static Stat GetReadStat() {
		Stat stat = new Stat(_readTimesAll);
		return stat;
	}
}
