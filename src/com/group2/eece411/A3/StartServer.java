package com.group2.eece411.A3;

import com.group2.eece411.KVServer;

@SuppressWarnings("unused")
public class StartServer {}
/*
	public static void main(String[] args) {

		int capacity;
		if (args.length != 0) {
			capacity = Integer.parseInt(args[0]);
		} else {
			capacity = 3000;
		}

		KVServer ks = new KVServer();
		ks.start();

		int min;
		if (args.length != 0) {
			min = Integer.parseInt(args[0]);
		} else {
			min = 0;
		}
		if (min != 0) {
			System.out.println("Server started @port:" + ks.getPort()
					+ " with capacity for " + capacity
					+ " entries, will stay alive for " + min + "minutes.");
			ks.stopMe(min * 60 * 1000);
		} else {
			System.out.println("Server started @port:" + ks.getPort()
					+ " with capacity for " + capacity
					+ " entries , will stay alive until shutdown is received.");
			ks.join();
		}
		System.out.println("Server shutting down...");
	}
}*/
