package com.group2.eece411.A3;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.DatatypeConverter;

import com.group2.eece411.KVClient;

public class StartClient {

	private static byte[] defaultKey = new byte[] { 99, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			99 };

	/**
	 * args[0] = numClient, args[1] = amount each, args[3] = server name
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		int numClient = 2;
		int amount = 100;
		// String host = "plonk.cs.uwaterloo.ca";
		// String host = "cs-planetlab3.cs.surrey.sfu.ca";
		// String host = "planetlab1.cs.ubc.ca";
		// String host = "pl1.cs.montana.edu";
		String host = "pl2.cs.montana.edu";
		// String host = "localhost";

		if (args.length != 0) {
			numClient = Integer.parseInt(args[0]);
			amount = Integer.parseInt(args[1]);
			host = args[3];
		}

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

		// get all node and print them out
		byte[] ret;
		InetAddress[] node = null;
		int[] nodePort = null;
		if ((ret = client.getAllNodes()) != null) {
			byte[] inet = new byte[4];
			byte[] port = new byte[4];
			node = new InetAddress[ret.length / 8];
			nodePort = new int[ret.length / 8];

			System.out.println("getAllNodes() passed! len:" + ret.length);

			for (int i = 0; i < ret.length / 8; i++) {
				System.arraycopy(ret, i * 8, inet, 0, 4);
				System.arraycopy(ret, i * 8 + 4, port, 0, 4);
				try {
					node[i] = InetAddress.getByAddress(inet);
					nodePort[i] = ByteBuffer.wrap(port).getInt();

					System.out.println(node[i].getHostAddress() + "@"
							+ nodePort[i]);
				} catch (UnknownHostException e) {
				}
			}
		} else {
			System.out.println("getAllNodes() failed!");
		}

		// finish
		client.close();

		// stress testing
		System.out.println("Stress Testing " + numClient + "client(s) at "
				+ amount + " packet each.");

		AtomicInteger p = new AtomicInteger(), n = new AtomicInteger(), l = new AtomicInteger(), v = new AtomicInteger();
		AtomicInteger bytesSent = new AtomicInteger();

		// set up some client
		ClientThread[] c = new ClientThread[numClient];
		for (int i = 0; i < c.length; i++) {
			c[i] = new ClientThread(amount, host, bytesSent, p, n, l, v, node,
					nodePort);
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
		double timeTaken = (System.currentTimeMillis() - time) / 1000.0;
		System.out.println("Time taken: " + timeTaken + " s. Bytes stored: "
				+ bytesSent.get() + " Speed: " + (bytesSent.get() / timeTaken)
				/ 1000.0 + " kBps");
	}
}
