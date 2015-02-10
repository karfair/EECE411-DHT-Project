package com.group2.eece411.A3;

import com.group2.eece411.KVServer;

public class StartServer {

	public static void main(String[] args) {

		KVServer ks = new KVServer(3000);
		ks.start();

		int min;
		if (args.length != 0) {
			min = Integer.parseInt(args[0]);
		} else {
			min = 0;
		}
		if (min != 0) {
			System.out.println("Server started @port:" + ks.getPort()
					+ ", will stay alive for " + min + "minutes.");
			ks.stopMe(min * 60 * 1000);
		} else {
			System.out.println("Server started @port:" + ks.getPort()
					+ ", will stay alive until shutdown is received.");
			ks.join();
		}
		System.out.println("Server shutting down...");
	}
}
