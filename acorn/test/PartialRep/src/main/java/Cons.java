// Console output utility
// - Doesn't serialize. It is by design

public class Cons
{
	public static int _ind_len = 0;
	public static StringBuilder _ind = new StringBuilder();

	public static void P(Object o) {
		if (_ind_len > 0) {
			System.out.println(o.toString().replaceAll("(?m)^", _ind.toString()));
		} else {
			System.out.println(o);
		}
	}

	// http://stackoverflow.com/questions/2925153/can-i-pass-an-array-as-arguments-to-a-method-with-variable-arguments-in-java
	public static void P(String fmt, Object... o) {
		P(String.format(fmt, o));
	}

	// nnl: no new line
	public static void Pnnl(Object o) {
		if (_ind_len > 0) {
			System.out.print(o.toString().replaceAll("(?m)^", _ind.toString()));
		} else {
			System.out.print(o);
		}
		System.out.flush();
	}

	public static void Pnnl(String fmt, Object... o) {
		Pnnl(String.format(fmt, o));
	}

	// MT: MeasureTime
	public static class MT implements AutoCloseable
	{
		//StackTraceElement _ste;
		long _start_time;

		public MT(String fmt, Object... o) {
			this(String.format(fmt, o));
		}

		public MT(String name) {
			//_ste = Thread.currentThread().getStackTrace()[2];
			P(name);
			_ind_len += 2;
			_ind.append("  ");
			_start_time = System.nanoTime();
		}

		@Override
		public void close() {
			double duration = (System.nanoTime() - _start_time) / 1000000.0;
			P(String.format("%.0f ms", duration));
			_ind_len -=2;
			_ind.setLength(_ind_len);

			//StringBuilder sb = new StringBuilder();
			//sb.append(Thread.currentThread().getId());
			//sb.append(" ");
			//sb.append(_ste.getClassName());
			//sb.append(".");
			//sb.append(_ste.getMethodName());
			//sb.append("()");
			//sb.append(" [");
			//sb.append(_ste.getFileName());
			//sb.append(":");
			//sb.append(_ste.getLineNumber());
			//sb.append("]");
			//System.err.printf("%s %f sec\n", sb.toString(), duration);
		}
	}
}
