package com.group2.eece411.A3;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.DatatypeConverter;

import com.group2.eece411.DHT;
import com.group2.eece411.KVClient;

public class StartClient {

	public static int multiplier = 1;
	public static int timeout = 2000;
	public static int tries = 5;
	public static int maxValueLength = 1000;

	public static Object minMaxLock = new Object();
	public static long maxP = 0, maxG = 0, maxR = 0;
	public static long minP = Long.MAX_VALUE, minG = Long.MAX_VALUE,
			minR = Long.MAX_VALUE;

	private static byte[] defaultKey = new byte[] { 99, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			99 };

	/**
	 * args[0] = numClient, args[1] = amount each, args[3] = server name
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		int numClient = 5;
		int amount = 10;
		String host = "planetlab1.cs.ubc.ca";
		// String host = "planetlab1.dojima.wide.ad.jp";
		// String host = "roam1.cs.ou.edu";
		// String host = "ricepl-2.cs.rice.edu";
		// String host = "cs-planetlab3.cs.surrey.sfu.ca";
		// String host = "planetlab1.cs.ubc.ca";
		// String host = "pl1.cs.montana.edu";
		// String host = "pl2.cs.montana.edu";
		// String host = "localhost";

		if (args.length != 0) {
			numClient = Integer.parseInt(args[0]);
			amount = Integer.parseInt(args[1]);
			host = args[2];
			tries = Integer.parseInt(args[3]);
			multiplier = Integer.parseInt(args[4]);
			timeout = Integer.parseInt(args[5]);
			maxValueLength = Integer.parseInt(args[6]);
		}

		// run a basic test
		KVClient client = new KVClient(host);

		client.setMultiplier(multiplier);
		client.setNumTries(tries);
		client.setTimeOut(timeout);

		System.out.println("put, put, get, remove, remove, get (key, 010103):");

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
		long time = System.currentTimeMillis();
		byte[] ret;
		InetAddress[] node = null;
		int[] nodePort = null;
		String nodes = "";
		String hash = "";

		if ((ret = client.getAllNodes()) != null) {
			byte[] inet = new byte[4];
			byte[] port = new byte[4];
			node = new InetAddress[ret.length / 8];
			nodePort = new int[ret.length / 8];

			System.out.println("getAllNodes() passed! len:" + ret.length
					+ " num nodes: " + ret.length / 8);

			for (int i = 0; i < ret.length / 8; i++) {
				System.arraycopy(ret, i * 8, inet, 0, 4);
				System.arraycopy(ret, i * 8 + 4, port, 0, 4);
				try {
					node[i] = InetAddress.getByAddress(inet);
					nodePort[i] = ByteBuffer.wrap(port).getInt();

					nodes += node[i].getHostAddress() + ":" + nodePort[i]
							+ "\n";
				} catch (UnknownHostException e) {
				}
				hash += DHT.positiveBigIntegerHash(inet).toString() + "\n";
			}
		} else {
			System.out.println("getAllNodes() failed!");
		}
		System.out.print(nodes + hash);
		System.out.println("getAllNodes() time taken: "
				+ (System.currentTimeMillis() - time) / 1000.0 + " s.");

		// finish
		client.close();

		// stress testing
		System.out.println("Stress Testing " + numClient + " client(s) at "
				+ amount + " packet each.");

		AtomicInteger p = new AtomicInteger(), n = new AtomicInteger(), l = new AtomicInteger(), v = new AtomicInteger(), rem = new AtomicInteger();
		AtomicInteger put = new AtomicInteger(), get = new AtomicInteger(), remove = new AtomicInteger();
		AtomicInteger bytesSent = new AtomicInteger();

		// set up some client
		ClientThread[] c = new ClientThread[numClient];
		for (int i = 0; i < c.length; i++) {
			c[i] = new ClientThread(amount, host, bytesSent, p, n, l, v, rem,
					node, nodePort, put, get, remove);
		}

		// start timing
		time = System.currentTimeMillis();

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
				+ " val error: " + v.get() + " remove error: " + rem.get());
		double timeTaken = (System.currentTimeMillis() - time) / 1000.0;
		System.out.println("Time taken: " + timeTaken + " s. Bytes stored: "
				+ bytesSent.get() + " Speed: " + (bytesSent.get() / timeTaken)
				/ 1000.0 + " kBps");
		double temp = numClient * amount * 1000;
		System.out.println("Ave put: " + (double) put.get() / temp
				+ " s. Ave get: " + (double) get.get() / temp
				+ " s. Ave remove: " + (double) remove.get() / temp
				+ " s. (not including failed attempts)");
		System.out.println("Max put: " + (double) maxP / 1000.0
				+ " s. Max get: " + (double) maxG / 1000.0 + " s. Max remove: "
				+ (double) maxR / 1000.0 + " s.");
		System.out.println("Min put: " + (double) minP / 1000.0
				+ " s. Min get: " + (double) minG / 1000.0 + " s. Min remove: "
				+ (double) minR / 1000.0 + " s.");
	}
}
