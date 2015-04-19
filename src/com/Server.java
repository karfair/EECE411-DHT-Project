package com;

import com.group2.eece411.DHT;
import com.group2.eece411.KVServer;

public class Server {

	/**
	 * if no arguments this will be the initial node (will start at port 7775),
	 * otherwise args[0] = <node to connect> to and args[1] = <its port>
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		int capacity = 4000;
		KVServer ks;
		if (args.length == 0) {
			ks = new KVServer(capacity, true, null, 0, null);
		} else if (args.length == 1) {
			ks = new KVServer(capacity, false, args[0],
					DHT.DEFAULT_DHT_TCP_PORT, null);
		} else {
			ks = new KVServer(capacity, false, args[0],
					DHT.DEFAULT_DHT_TCP_PORT, args[1]);
		}

		ks.start();
		System.out.println("Server started @port:" + ks.getPort()
				+ " with capacity for " + capacity
				+ " entries , will stay alive until shutdown is received.");
		ks.join();
	}
}
