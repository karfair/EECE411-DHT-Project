package com.group2.eece411.A3;

import com.group2.eece411.KVServer;

public class StartServer {

	public static void main(String[] args) {

		KVServer ks = new KVServer(3000);
		ks.start();

		int sec;
		if (args.length != 0) {
			sec = Integer.parseInt(args[0]);
		}
		sec = 60 * 60;
		System.out.println("Server started @port:" + ks.getPort()
				+ ", will stay alive for " + sec + "s.");
		try {
			Thread.sleep(sec * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Server shutting down...");
		ks.stopMe();
	}
}
