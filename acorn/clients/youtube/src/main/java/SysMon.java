// A light-weight system monitor, not forking a subprocess everytime it checks.

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.lang.InterruptedException;

public class SysMon {
	public static void Test() throws FileNotFoundException, IOException, InterruptedException {
		while (true) {
			double cpu_usage = Cpu();
			RxTx eth0_rxtx = Eth0RxTx();
			Cons.P("cpu=%.2f%% %s", cpu_usage, eth0_rxtx);
			Thread.sleep(1000);
		}
	}

	// Single-threaded. No synchronization needed.

	private static long _prev_usr = -1;
	private static long _prev_nice = -1;
	private static long _prev_system = -1;
	private static long _prev_idle = -1;
	private static long _prev_total = -1;

	public static double Cpu() throws FileNotFoundException, IOException {
		// http://stackoverflow.com/questions/3769405/determining-cpu-utilization
		try (BufferedReader br = new BufferedReader(new FileReader("/proc/stat"))) {
			double cpu_usage = 0.0;
			String line = null;
			while ((line = br.readLine()) != null) {
				if (! line.startsWith("cpu "))
					continue;
				// cpu  90139 726 6254 110933863 37994 0 11 22067 0 0
				//
				// man proc
				//   user   (1) Time spent in user mode.
				//   nice   (2) Time spent in user mode with low priority (nice).
				//   system (3) Time spent in system mode.
				//   idle   (4) Time spent in the idle task.  This value should be USER_HZ times the second entry in the /proc/uptime pseudo-file.
				//   iowait (since Linux 2.5.41)
				//          (5) Time waiting for I/O to complete.
				//   irq (since Linux 2.6.0-test4)
				//          (6) Time servicing interrupts.
				//   softirq (since Linux 2.6.0-test4)
				//          (7) Time servicing softirqs.
				//   steal (since Linux 2.6.11)
				//          (8) Stolen time, which is the time spent in other operating systems when running in a virtualized environment
				//   guest (since Linux 2.6.24)
				//          (9) Time spent running a virtual CPU for guest operating systems under the control of the Linux kernel.
				//   guest_nice (since Linux 2.6.33)
				//          (10) Time spent running a niced guest (virtual CPU for guest operating systems under the control of the Linux kernel).
				String[] t = line.split(" +");
				if (t.length != 11)
					throw new RuntimeException(String.format("Unexpected format: %s", line));
				long usr = Long.parseLong(t[1]);
				long nice = Long.parseLong(t[2]);
				long system = Long.parseLong(t[3]);
				long idle = Long.parseLong(t[4]);
				long total = usr + nice + system + idle;
				//Cons.P("%d %d %d %d", usr, nice, system, idle, total);

				if (_prev_total != -1) {
					cpu_usage = (double)((usr + nice + system) - (_prev_usr + _prev_nice + _prev_system))
						/ (total - _prev_total);
				}

				_prev_usr = usr;
				_prev_nice = nice;
				_prev_system = system;
				_prev_idle = idle;
				_prev_total = total;
				break;
			}
			return 100.0 * cpu_usage;
		}
	}

	private static RxTx _prev_rt = null;

	public static RxTx Eth0RxTx() throws IOException, InterruptedException {
		RxTx diff = null;

		RxTx rt = _GetEth0RxTx();
		if (_prev_rt == null) {
			diff = new RxTx(0, 0);
		} else {
			diff = new RxTx(rt.rx - _prev_rt.rx, rt.tx - _prev_rt.tx);
		}
		_prev_rt = rt;

		return diff;
	}

	static class RxTx {
		long rx = 0;
		long tx = 0;

		RxTx(long rx, long tx) {
			this.rx = rx;
			this.tx = tx;
		}

		@Override
		public String toString() {
			return String.format("rx=%d tx=%d", rx, tx);
		}
	}

	private static RxTx _GetEth0RxTx() throws IOException, InterruptedException {
		// http://stackoverflow.com/questions/792024/how-to-execute-system-commands-linux-bsd-using-java
		Runtime r = Runtime.getRuntime();
		Process p = r.exec("ifconfig eth0");
		p.waitFor();
		BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line = "";
		long rx = -1;
		long tx = -1;
		while ((line = b.readLine()) != null) {
			// RX bytes:80098259 (80.0 MB)  TX bytes:62102148 (62.1 MB)
			{
				String[] t = line.split("RX bytes:");
				if (t.length == 2) {
					String[] t1 = t[1].split(" ");
					rx = Long.parseLong(t1[0]);
				}
			}
			{
				String[] t = line.split("TX bytes:");
				if (t.length == 2) {
					String[] t1 = t[1].split(" ");
					tx = Long.parseLong(t1[0]);
				}
			}
		}
		b.close();
		if (rx == -1 || tx == -1)
			throw new RuntimeException("Unable to get RX and TX bytes");
		return new RxTx(rx, tx);
	}
}
