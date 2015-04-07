package com.group2.eece411.A4;

import com.group2.eece411.DHT;

public class TestDHT {

	public static void main(String[] args) {
		DHT dht = new DHT(true, null, 0, 0, null);

		// this node will sit at
		// "pl1.cs.montana.edu" port 7775
		dht.start();
		System.out.println("port: " + dht.getLocalPort());
	}
}
