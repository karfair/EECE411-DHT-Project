package com.group2.eece411;

import java.net.InetAddress;

public interface RequestListener {
	public void handleRequest(byte[] uniqueRequestID, byte[] request,
			InetAddress srcAddr, int srcPort, boolean routeMsgTo,
			InetAddress srcServer, int serverPort);
}
