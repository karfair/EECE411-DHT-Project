package com.group2.eece411.A5;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import javax.xml.bind.DatatypeConverter;

import com.group2.eece411.Config.Code.Command;
import com.group2.eece411.DHT;
import com.group2.eece411.KVClient;
import com.group2.eece411.UDPClient;

public class KillRandom {
	private static int multiplier = 2;
	private static int timeout = 200;
	private static int tries = 3;

	private static byte[] defaultKey = new byte[] { 99, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			99 };

	public static void main(String[] args) {
		// String host = "128.208.4.199";
		// String host = "planetlab1.dojima.wide.ad.jp";
		// String host = "roam1.cs.ou.edu";
		// String host = "ricepl-2.cs.rice.edu";
		// String host = "cs-planetlab3.cs.surrey.sfu.ca";
		//String host = "planetlab1.cs.ubc.ca";
		// String host = "pl1.cs.montana.edu";
		// String host = "pl2.cs.montana.edu";
		// String host = "localhost";
		// String host = "128.42.142.45";

		// group a
		String host = "plonk.cs.uwaterloo.ca";

		// group f
		// String host = "planetlab3.cesnet.cz";

		// String host = "planetlab1.cs.ubc.ca";
		// String host = "204.8.155.227";
		
		// skip 1, kill 2, skip 1, etc...
		int skip = 1;
		int kill = 2;

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
			System.exit(1);
		}
		System.out.print(nodes + hash);
		System.out.println("getAllNodes() time taken: "
				+ (System.currentTimeMillis() - time) / 1000.0 + " s.");
		
		
		// killing nodes
		UDPClient udp = new UDPClient(host, 6772);
		byte[] cmd = new byte[1];
		cmd[0] = Command.SHUTDOWN;
		
		int cycleLength = kill + skip;
		int maxCycles = (node.length / cycleLength) * cycleLength;
		
		for (int i = 0; i < maxCycles; i++) {
			if (i % cycleLength >= skip) {
				udp.changeDest(node[i].getHostAddress(), 6772);
				try {
					udp.sendAndWaitFor(cmd, 1, 1, 1);
				} catch (IOException e) {
					// exception thrown, this is normal
				}
			}
		}

		// finish
		client.close();
		udp.close();
	}
}
