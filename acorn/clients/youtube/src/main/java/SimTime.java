public class SimTime
{
	private static long startTime = -1;

	public static void SetStartTime(long startTime_) {
		startTime = startTime_;

		long timeToStartTime = startTime - System.currentTimeMillis();
		if (timeToStartTime > 0)
			Cons.P("startTime %s is %d ms in the future", startTime, timeToStartTime);
		else
			throw new RuntimeException(String.format("Unexpected: timeToStartTime=%d", timeToStartTime));
	}
}
