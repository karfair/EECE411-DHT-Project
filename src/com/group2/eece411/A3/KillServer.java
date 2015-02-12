package com.group2.eece411.A3;

import com.group2.eece411.KVClient;

public class KillServer {
	public static void main(String[] args) {
		String targetServer = "plonk.cs.uwaterloo.ca";
		// String targetServer = "localhost";
		if (args.length != 0) {
			targetServer = args[0];
		}
		KVClient u = new KVClient(targetServer);
		u.kill();
		u.close();
	}
}
