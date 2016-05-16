import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.NetworkInterface;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
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
}
