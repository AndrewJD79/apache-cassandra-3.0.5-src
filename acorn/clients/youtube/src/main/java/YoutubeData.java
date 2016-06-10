import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

class YoutubeData {
	public static long numReqs = -1;

	public static void Load() throws Exception {
		String fn = String.format("%s/%s"
				, Conf.acornYoutubeOptions.dn_data
				, Conf.acornYoutubeOptions.fn_youtube_reqs);
		try (Cons.MT _ = new Cons.MT("Loading data file %s in the background ...", fn)) {
			File file = new File(fn);
			FileInputStream fis = new FileInputStream(file);
			BufferedInputStream bis = new BufferedInputStream(fis);

			long numTweets = _ReadLong(bis);
			Cons.P("Total number of read and write requests: %d", numTweets);

			Req lastReq = null;
			for (long i = 0; i < numTweets; i ++) {
				Req r = new Req(bis);
				lastReq = r;
				//if (i == 0)
				//	Cons.P(r);

				// Limit the number of requests. Useful for debugging.
				if (i == Conf.acornYoutubeOptions.max_requests) {
					//Cons.P(r);
					// 2012-02-15 13:36:22
					break;
				}

				// Load the req only when the local DC is the closest DC from the
				// request.
				if (DC.IsLocalDcTheClosestToReq(r))
					allReqs.put(r);
			}
			SimTime.Init(lastReq);

			// Delay read request time by like 1.5 sec in simulation time into the
			// future so that they are actually read by remote datacenters.
			long simulatedTimeInterval = SimTime.ToSimulatedTimeInterval(Conf.acornYoutubeOptions.read_req_delay_in_simulation_time_in_ms);
			long lastReadSimulatedTime = -1;
			for (Req r: allReqs) {
				// Force calculation. Should be good for the jitter.
				r.GetSimulatedTime();

				if (r.type == Req.Type.W)
					continue;

				r.simulatedTime += simulatedTimeInterval;
				lastReadSimulatedTime = r.simulatedTime;
			}

			// Check the last read simulated time
			Cons.P("lastReadSimulatedTime=%d %s", lastReadSimulatedTime
					, (new SimpleDateFormat("yyMMdd-HHmmss")).format(new Date(lastReadSimulatedTime)));

			numReqs = allReqs.size();
			Cons.P("Loaded %d requests", numReqs);
			// Takes 906 ms to read a 68MB file. About 600 ms with a warm cache.
			//   /home/ubuntu/work/acorn-data/150505-104600-tweets
			//   Total number of read and write requests: 556609
			//
			// 12 sec for the full file with a warn cache.
		}
	}

	public static long NumReqs() {
		return numReqs;
	}

	static class Req {
		// Tweet ID. Not used for this application.
		long id;
		// User ID. Video uploader is constructed from uid. Not used for this
		// application. Alreay used for generating the data file.
		long uid;

		// In the format of 2010-08-16 01:47:12
		String createdAt;

		double geoLati;
		double geoLongi;
		// YouTube video ID
		String vid;
		// Video uploader. Data file has this as long. Convert to String so that
		// Cassandra can treat it as a string.
		String videoUploader;
		List<String> topics;

		// This is calculated from createdAt when requested
		long simulatedTime = -1;

		enum Type {
			NA, // Not assigned yet
			W,
			R,
		}
		Type type;

		public Req(BufferedInputStream bis) throws Exception {
			id = _ReadLong(bis);
			uid = _ReadLong(bis);
			createdAt = _ReadStr(bis);
			geoLati = _ReadDouble(bis);
			geoLongi = _ReadDouble(bis);
			vid = _ReadStr(bis);
			videoUploader = Long.toString(_ReadLong(bis));

			long numTopics = _ReadLong(bis);
			//Cons.P("numTopics=%d", numTopics);
			topics = new ArrayList<String>();
			for (long i = 0; i < numTopics; i ++)
				topics.add(_ReadStr(bis));

			// http://stackoverflow.com/questions/5878952/cast-int-to-enum-in-java
			type = Type.values()[_ReadInt(bis)];
			if (type != Type.W && type != Type.R)
				throw new RuntimeException(String.format("Unexpected: type=%s", type));
			//Cons.P("type=%s", type);
		}

		public long GetSimulatedTime() throws Exception {
			if (simulatedTime != -1)
				return simulatedTime;

			// SimpleDateFormat is not thread safe. Oh well.
			// http://stackoverflow.com/questions/10411944/java-text-simpledateformat-not-thread-safe
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date d = df.parse(createdAt);
			simulatedTime = d.getTime();
			return simulatedTime;
		}

		@Override
		public String toString() {
			return ReflectionToStringBuilder.toString(this);
		}
	}
	// A thread-safe blocking queue
	public static BlockingQueue<Req> allReqs = new LinkedBlockingQueue<Req>();

	private static int _ReadInt(BufferedInputStream bis) throws Exception {
		// 32-bit int
		byte[] bytes = new byte[4];
		int bytesRead = bis.read(bytes);
		if (bytesRead != 4)
			throw new RuntimeException(String.format("Unexpected: bytesRead=%d", bytesRead));

		// http://stackoverflow.com/questions/5616052/how-can-i-convert-a-4-byte-array-to-an-integer
		return ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
	}

	private static long _ReadLong(BufferedInputStream bis) throws Exception {
		// 64-bit int
		byte[] bytes = new byte[8];
		int bytesRead = bis.read(bytes);
		if (bytesRead != 8)
			throw new RuntimeException(String.format("Unexpected: bytesRead=%d", bytesRead));

		// http://stackoverflow.com/questions/5616052/how-can-i-convert-a-4-byte-array-to-an-integer
		return ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
	}

	private static String _ReadStr(BufferedInputStream bis) throws Exception {
		int size = (int) _ReadLong(bis);

		byte[] bytes = new byte[size];
		int bytesRead = bis.read(bytes);
		if (bytesRead != size)
			throw new RuntimeException(String.format("Unexpected: bytesRead=%d size=%d", bytesRead, size));
		// http://stackoverflow.com/questions/17354891/java-bytebuffer-to-string
		return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
	}

	private static double _ReadDouble(BufferedInputStream bis) throws Exception {
		// 64-bit int
		byte[] bytes = new byte[8];
		int bytesRead = bis.read(bytes);
		if (bytesRead != 8)
			throw new RuntimeException(String.format("Unexpected: bytesRead=%d", bytesRead));

		// http://stackoverflow.com/questions/2905556/how-can-i-convert-a-byte-array-into-a-double-and-back
		return ByteBuffer.wrap(bytes).getDouble();
	}
}
