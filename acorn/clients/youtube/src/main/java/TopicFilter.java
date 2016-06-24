import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


class TopicFilter {
	private static boolean _initialized = false;
	private static Set<String> _topics = new HashSet<String>();


	public static void _Init() throws Exception {
		if (_initialized)
			return;

		String fn = String.format("%s/%s"
				, Conf.acornYoutubeOptions.dn_data
				, Conf.acornYoutubeOptions.fn_topic_filter);

		try (BufferedReader br = new BufferedReader(new FileReader(fn))) {
			String line;
			while ((line = br.readLine()) != null) {
				if (line.length() > 0 && line.charAt(0) == '#')
					continue;

				String[] t = line.split(" ");
				if (t.length != 2)
					throw new RuntimeException(String.format("Unexpected line=[%s]", line));
				_topics.add(t[0]);
			}
		}
		_initialized = true;

		Cons.P("Loaded %d topic filter items", _topics.size());
	}


	public static boolean Blacklisted(List<String> topics) throws Exception {
		_Init();

		for (String t: topics) {
			if (_topics.contains(t))
				return true;
		}
		return false;
	}
}
