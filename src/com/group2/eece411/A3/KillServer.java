package com.group2.eece411.A3;

import com.group2.eece411.KVClient;

public class KillServer {
	public static void main(String[] args) {
		KVClient u = new KVClient("plonk.cs.uwaterloo.ca");
		u.kill();
		u.close();
	}

}
