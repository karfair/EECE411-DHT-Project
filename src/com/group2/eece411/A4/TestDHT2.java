package com.group2.eece411.A4;

import com.group2.eece411.DHT;

@Deprecated
public class TestDHT2 {

	public static void main(String[] args) {
		DHT dht = new DHT(false, "pl1.cs.montana.edu", 7775, 0, null);
		dht.start();
		System.out.println("port: " + dht.getLocalPort());

		// pl1.cs.montana.edu
		// pl2.cs.montana.edu
		// pl2.cs.unm.edu
	}

}
