import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.NetworkInterface;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.TimeZone;


class Util {
	static private String _hostname = null;

	// We take only the first part of hostname, e.g., abc from abc.cc.gatech.edu
	static String Hostname() throws java.net.UnknownHostException {
		if (_hostname != null)
			return _hostname;
		java.net.InetAddress addr = java.net.InetAddress.getLocalHost();
		_hostname = (addr.getHostName().split("\\."))[0];
		return _hostname;
	}

	static private String _eth0_ip = null;
	static String GetEth0IP() throws java.net.SocketException {
		if (_eth0_ip != null)
			return _eth0_ip;
		Pattern ipv4_pattern = Pattern.compile("[1-9][0-9]?[0-9]?\\.[1-9][0-9]?[0-9]?\\.[1-9][0-9]?[0-9]?\\.[1-9][0-9]?[0-9]?");

		for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
			NetworkInterface intf = en.nextElement();
			if (intf.getName().equals("eth0")) {
				for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements(); ) {
					String ip = enumIpAddr.nextElement().toString();
					if (ip.charAt(0) == '/')
						ip = ip.substring(1);
					Matcher matcher = ipv4_pattern.matcher(ip);
					if (matcher.find()) {
						//System.out.println(ip);
						_eth0_ip = ip;
						return _eth0_ip;
					}
				}
			}
		}
		throw new RuntimeException("Unable to get the IPv4 address");
	}

	static String CurDateTime() {
		// http://stackoverflow.com/questions/308683/how-can-i-get-the-current-date-and-time-in-utc-or-gmt-in-java
		SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd-HHmmss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		Date now = new Date();
		return sdf.format(now);
	}

	static class RxTx {
		int rx;
		int tx;

		RxTx(int rx, int tx) {
			this.rx = rx;
			this.tx = tx;
		}

		@Override
		public String toString() {
			return String.format("rx=%d tx=%d", rx, tx);
		}
	}

	static RxTx GetEth0RxTx() throws IOException, InterruptedException {
		// http://stackoverflow.com/questions/792024/how-to-execute-system-commands-linux-bsd-using-java
		Runtime r = Runtime.getRuntime();
		Process p = r.exec("ifconfig eth0");
		p.waitFor();
		BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line = "";
		int rx = -1;
		int tx = -1;
		while ((line = b.readLine()) != null) {
			// RX bytes:80098259 (80.0 MB)  TX bytes:62102148 (62.1 MB)
			{
				String[] t = line.split("RX bytes:");
				if (t.length == 2) {
					String[] t1 = t[1].split(" ");
					rx = Integer.parseInt(t1[0]);
				}
			}
			{
				String[] t = line.split("TX bytes:");
				if (t.length == 2) {
					String[] t1 = t[1].split(" ");
					tx = Integer.parseInt(t1[0]);
				}
			}
		}
		b.close();
		if (rx == -1 || tx == -1)
			throw new RuntimeException("Unable to get RX and TX bytes");
		return new RxTx(rx, tx);
	}

	// http://stackoverflow.com/questions/1149703/how-can-i-convert-a-stack-trace-to-a-string
	public static String GetStackTrace(Throwable t) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		t.printStackTrace(pw);
		return sw.toString();
	}

	public static String BuildHeader(String fmt, int leftMargin, String... args) {
		// Get the end position of column header. fmt specifiers consist of float,
		// integer, or string
		Pattern p = Pattern.compile("%(([-+]?[0-9]*\\.?[0-9]*f)|([-+]?[0-9]*d)|([-+]?[0-9]*s))");
		Matcher m = p.matcher(fmt);
		List<Integer> nameEndPos = new ArrayList();
		int ep = leftMargin;
		while (m.find()) {
			// m.group() returns for example %5.1f
			//
			// remove all characters from .
			//   .replaceAll("\\..*", "")
			// remove all non-numeric characters
			//   .replaceAll("\\D", "")

			int width = Integer.parseInt(m.group().substring(1).replaceAll("\\..*", "").replaceAll("\\D", ""));
			// System.out.printf("[%s] %d\n", m.group(), width);

			if (ep != leftMargin)
				ep ++;
			ep += width;
			nameEndPos.add(ep);
		}
		//System.out.printf("nameEndPos:\n");
		//for (int i: nameEndPos)
		//	System.out.printf("  %d\n", i);
		//System.out.printf("args:\n");
		//for (String s: args)
		//	System.out.printf("  %s\n", s);

		StringBuilder colmneNamesFlat = new StringBuilder("#");
		for (String s: args)
			colmneNamesFlat.append(" ").append(s);

		// Header lines
		List<StringBuilder> headerLines = new ArrayList();
		for (int i = 0; i < args.length; i ++) {
			boolean fit = false;
			for (StringBuilder hl: headerLines) {
				if (hl.length() + 1 + args[i].length() > nameEndPos.get(i))
					continue;

				while (hl.length() + 1 + args[i].length() < nameEndPos.get(i))
					hl.append(" ");
				hl.append(" ").append(args[i]);
				fit = true;
				break;
			}
			if (fit)
				continue;

			StringBuilder hl = new StringBuilder("#");
			while (hl.length() + 1 + args[i].length() < nameEndPos.get(i))
				hl.append(" ");
			hl.append(" ").append(args[i]);
			headerLines.add(hl);
		}
		//System.out.printf("headerLines:\n");
		//for (StringBuilder hl: headerLines)
		//	System.out.printf("  %s\n", hl);

		// Indices for column headers starting from 1 for easy gnuplot indexing
		List<StringBuilder> headerLineIndices = new ArrayList();
		for (int i = 0; i < args.length; i ++) {
			String idxstr = Integer.toString(i + 1);
			boolean fit = false;
			for (StringBuilder hli: headerLineIndices) {
				if (hli.length() + 1 + idxstr.length() > nameEndPos.get(i))
					continue;

				while (hli.length() + 1 + idxstr.length() < nameEndPos.get(i))
					hli.append(" ");
				hli.append(" ").append(idxstr);
				fit = true;
				break;
			}
			if (fit)
				continue;

			StringBuilder hli = new StringBuilder("#");
			while (hli.length() + 1 + idxstr.length() < nameEndPos.get(i))
				hli.append(" ");
			hli.append(" ").append(idxstr);
			headerLineIndices.add(hli);
		}
		//System.out.printf("headerLineIndices:\n");
		//for (StringBuilder hli: headerLineIndices)
		//	System.out.printf("  %s\n", hli);

		StringBuilder header = new StringBuilder();
		//header.append(colmneNamesFlat);
		//header.append("\n#");

		for (StringBuilder l: headerLines)
			header.append("\n").append(l);
		for (StringBuilder l: headerLineIndices)
			header.append("\n").append(l);

		return header.toString();
	}
}
