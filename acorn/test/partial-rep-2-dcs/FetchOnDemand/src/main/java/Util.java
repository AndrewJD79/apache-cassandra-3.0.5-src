import java.net.NetworkInterface;
import java.net.InetAddress;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Util {
	static private String _hostname = null;

	// we take only the first part of hostname, e.g., abc from abc.cc.gatech.edu
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
}
