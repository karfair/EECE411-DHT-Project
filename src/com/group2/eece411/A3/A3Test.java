package com.group2.eece411.A3;

import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.DatatypeConverter;

import com.group2.eece411.KVClient;
import com.group2.eece411.KVServer;

/**
 * Start both server and client
 * 
 * @author Phil
 *
 */
/*
public class A3Test {
	public static void main(String[] args) {
		byte[] defaultKey = new byte[] { 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 99 };
		int numClient = 15;
		int amount = 1000;
		String host = "localhost";

		if (args.length != 0) {
			numClient = Integer.parseInt(args[0]);
			amount = Integer.parseInt(args[1]);
			host = args[3];
		}

		// starts server
		KVServer server = new KVServer();
		server.start();

		// run a basic test
		KVClient client = new KVClient(host);
		System.out.println("put, put, get, remove, remove, get:");

		// 2 put test
		if (client.put(defaultKey, new byte[] { 1, 2, 3 })) {
			System.out.println("1st put: success");
		} else {
			System.out.println("1st put: failed");
		}
		if (client.put(defaultKey, new byte[] { 1, 1, 3 })) {
			System.out.println("2nd put: success");
		} else {
			System.out.println("2nd put: failed");
		}

		// 2 get test
		System.out.println("1st get: "
				+ DatatypeConverter.printHexBinary(client.get(defaultKey)));
		System.out.println("2nd get: "
				+ DatatypeConverter.printHexBinary(client.get(defaultKey)));

		// 2 remove test
		if (client.remove(defaultKey)) {
			System.out.println("1st remove: success");
		} else {
			System.out.println("1st remove: failed");
		}
		if (!client.remove(defaultKey)) {
			System.out.println("2nd remove: success");
		} else {
			System.out.println("2nd remove: failed");
		}

		// final get test
		if (client.get(defaultKey) == null) {
			System.out.println("3rd get: success");
		}
		client.close();

		// stress testing
		System.out.println("Stress Testing " + numClient + "client(s) at "
				+ amount + " packet each.");

		AtomicInteger p = new AtomicInteger(), n = new AtomicInteger(), l = new AtomicInteger(), v = new AtomicInteger();
		AtomicInteger bytesSent = new AtomicInteger();

		// set up some client
		ClientThread[] c = new ClientThread[numClient];
		for (int i = 0; i < c.length; i++) {
			c[i] = new ClientThread(amount, host, bytesSent, p, n, l, v);
		}

		// start timing
		long time = System.currentTimeMillis();

		// start client
		for (int i = 0; i < c.length; i++) {
			c[i].start();
		}
		// wait for them to finish
		for (int i = 0; i < c.length; i++) {
			try {
				c[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// print out some stats
		System.out.println("Stress test completed. put error: " + p.get()
				+ " null error: " + n.get() + " len error: " + l.get()
				+ " val error: " + v.get());
		System.out.println("Time taken: " + (System.currentTimeMillis() - time)
				/ 1000.0 + " s. Bytes stored: " + bytesSent.get());

		// shut down the server
		server.stopMe();
	}
}*/
