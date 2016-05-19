import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

class YoutubeData {
	public static void Load() throws Exception {
		String fn = String.format("%s/%s"
				, Conf.acornYoutubeOptions.dn_data
				, Conf.acornYoutubeOptions.fn_youtube_reqs);
		try (Cons.MT _ = new Cons.MT("Loading data file %s ...", fn)) {
			File file = new File(fn);
			FileInputStream fis = new FileInputStream(file);
			BufferedInputStream bis = new BufferedInputStream(fis);

			long numTweets = _ReadLong(bis);
			Cons.P("Total number of read and write requests: %d", numTweets);

			for (long i = 0; i < numTweets; i ++) {
				Req r = new Req(bis);
				//if (i == 0)
				//	Cons.P(r);

				// Load the req only when the local DC is the closest DC from the
				// request.
				if (DC.IsLocalDcTheClosestToReq(r))
					allReqs.add(r);
			}

			Cons.P("Loaded %d requests", allReqs.size());
			// Takes 906 ms to read a 68MB file. About 600 ms with a warm cache.
			//   /home/ubuntu/work/acorn-data/150505-104600-tweets
			//   Total number of read and write requests: 556609
			//
			// 12 sec for the full file with a warn cache.
		}
	}

	static class Req {
		// Tweet ID. Not used for this application.
		long id;
		// User ID. Video uploader is constructed from uid.
		long uid;

		String createdAt;
		double geoLati;
		double geoLongi;
		// YouTube video ID
		String vid;
		// Video uploader
		long videoUploader;
		List<String> topics;

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
			videoUploader = _ReadLong(bis);

			long numTopics = _ReadLong(bis);
			//Cons.P("numTopics=%d", numTopics);
			topics = new ArrayList<String>();
			for (long i = 0; i < numTopics; i ++)
				topics.add(_ReadStr(bis));

			// http://stackoverflow.com/questions/5878952/cast-int-to-enum-in-java
			type = Type.values()[_ReadInt(bis)];
			//Cons.P("type=%s", type);
		}

		@Override
		public String toString() {
			return ReflectionToStringBuilder.toString(this);
		}
	}
	private static List<Req> allReqs = new ArrayList<Req>();

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
