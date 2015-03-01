package com.group2.eece411.A4;

import com.group2.eece411.KVServer;

public class A4 {

	/**
	 * if no arguments this will be the initial node (will start at port 7775),
	 * otherwise args[0] = <node to connect> to and args[1] = <its port>
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		int capacity = 3000;
		KVServer ks;
		if (args.length == 0) {
			ks = new KVServer(capacity, true, null, 0);
		} else {
			ks = new KVServer(capacity, false, args[0],
					Integer.parseInt(args[1]));
		}

		ks.start();
		System.out.println("Server started @port:" + ks.getPort()
				+ " with capacity for " + capacity
				+ " entries , will stay alive until shutdown is received.");
		ks.join();
	}
}
