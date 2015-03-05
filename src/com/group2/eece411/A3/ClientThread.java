package com.group2.eece411.A3;

import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.group2.eece411.Config.Code;
import com.group2.eece411.KVClient;

public class ClientThread extends Thread {
	private int amountToSend;
	private Random r = new Random();
	private KVClient client;

	private int bytesSent = 0, p = 0, n = 0, l = 0, v = 0;
	private AtomicInteger ab, ap, an, al, av;

	private InetAddress[] nodes;
	private int[] port;

	public ClientThread(int amountToSend, String host, AtomicInteger bytesSent,
			AtomicInteger p, AtomicInteger n, AtomicInteger l, AtomicInteger v,
			InetAddress[] selection, int[] port) {
		super();
		this.amountToSend = amountToSend;
		// client = new KVClient(host);

		ab = bytesSent;
		ap = p;
		an = n;
		al = l;
		av = v;

		this.nodes = selection;
		this.port = port;
	}

	@Override
	public void run() {
		KVClient[] clients = new KVClient[nodes.length];
		for (int i = 0; i < clients.length; i++) {
			clients[i] = new KVClient(nodes[i].getHostAddress(), port[i]);
		}
		for (int i = 0; i < amountToSend; i++) {
			byte[] key = keygen();
			byte[] val = valgen();
			bytesSent += Code.KEY_LENGTH + Code.VALUE_LENGTH_LENGTH
					+ val.length;

			client = clients[r.nextInt(nodes.length)];
			// see if put is working
			if (!client.put(key, val)) {
				System.out.println("put error!");
				p++;
				continue;
			}

			client = clients[r.nextInt(nodes.length)];
			// see if the returned value is null
			byte[] retVal = client.get(key);
			if (retVal == null) {
				System.out.println("null error!");
				n++;
				continue;
			}

			// check if the returned len is equal
			if (retVal.length != val.length) {
				System.out.println("length error!");
				l++;
				continue;
			}

			// check if the returned bytes[] are equal
			if (!valEquals(val, retVal)) {
				System.out.println("value error!");
				v++;
				continue;
			}
		}
		client.close();

		ab.addAndGet(bytesSent);
		ap.addAndGet(p);
		an.addAndGet(n);
		al.addAndGet(l);
		av.addAndGet(v);
	}

	private byte[] keygen() {
		byte[] key = new byte[Code.KEY_LENGTH];
		r.nextBytes(key);
		return key;
	}

	private byte[] valgen() {
		byte[] val = new byte[r.nextInt(15000) + 1];
		r.nextBytes(val);
		return val;
	}

	private static boolean valEquals(byte[] val1, byte[] val2) {
		for (int j = 0; j < val1.length; j++) {
			if (val1[j] != val2[j]) {
				return false;
			}
		}
		return true;
	}
}