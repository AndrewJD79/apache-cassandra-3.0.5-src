
class XDcTrafficMon {
	private static MonThread _mt = new MonThread();
	private static Thread _thrMt = new Thread(_mt);

	public static void Start() throws Exception {
		_thrMt.start();
	}

	private static boolean stopRequested = false;

	public static void Stop() throws Exception {
		stopRequested = true;
		synchronized (_mt) {
			_mt.notifyAll();
		}
		_thrMt.join();
	}

	private static Util.RxTx rt = null;
	private static Util.RxTx rtLast = null;

	private static class MonThread implements Runnable
	{
		public void run() {
			try {
				while (true) {
					synchronized (this) {
						Util.RxTx rt0 = Util.GetEth0RxTx();
						if (rt == null) {
							rt = rtLast = rt0;
						} else {
							rt = rt0;
						}
						//Cons.P(rt);
					}

					synchronized (this) {
						wait(100);
					}
					if (stopRequested)
						break;
				}
			} catch (Exception e) {
				System.out.printf("Exception: %s\n%s\n", e, Util.GetStackTrace(e));
				System.exit(1);
			}
		}
	}

	public static Util.RxTx Get() {
		synchronized (_mt) {
			Util.RxTx rt0 = new Util.RxTx(rt.rx - rtLast.rx, rt.tx - rtLast.tx);
			rtLast = rt;
			return rt0;
		}
	}

	public static void Test() throws Exception {
		Start();
		Thread.sleep(500);
		Cons.P("AA %s", Get());
		Thread.sleep(500);
		Cons.P("AA %s", Get());
		Thread.sleep(500);
		Stop();
		Cons.P("AA %s", Get());
	}
}
