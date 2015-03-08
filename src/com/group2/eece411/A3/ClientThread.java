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

	private int bytesSent = 0, p = 0, n = 0, l = 0, v = 0, rem = 0;
	private long put = 0, get = 0, remove = 0;
	private long maxP = 0, maxG = 0, maxR = 0;
	private long minP = Long.MAX_VALUE, minG = Long.MAX_VALUE,
			minR = Long.MAX_VALUE;
	private AtomicInteger ab, ap, an, al, av, ar;
	private AtomicInteger putTime, getTime, removeTime;

	private InetAddress[] nodes;
	private int[] port;

	public ClientThread(int amountToSend, String host, AtomicInteger bytesSent,
			AtomicInteger p, AtomicInteger n, AtomicInteger l, AtomicInteger v,
			AtomicInteger r, InetAddress[] selection, int[] port,
			AtomicInteger putTime, AtomicInteger getTime,
			AtomicInteger removeTime) {
		super();
		this.amountToSend = amountToSend;
		// client = new KVClient(host);

		ab = bytesSent;
		ap = p;
		an = n;
		al = l;
		av = v;
		ar = r;

		this.putTime = putTime;
		this.getTime = getTime;
		this.removeTime = removeTime;

		this.nodes = selection;
		this.port = port;
	}

	@Override
	public void run() {
		KVClient[] clients;
		if (nodes.length <= 5) {
			clients = new KVClient[nodes.length];
			for (int i = 0; i < clients.length; i++) {
				clients[i] = new KVClient(nodes[i].getHostAddress(), port[i]);
				clients[i].setMultiplier(StartClient.multiplier);
				clients[i].setNumTries(StartClient.tries);
				clients[i].setTimeOut(StartClient.timeout);
			}
		} else {
			clients = new KVClient[5];
			for (int i = 0; i < clients.length; i++) {
				int which = r.nextInt(nodes.length);
				clients[i] = new KVClient(nodes[which].getHostAddress(),
						port[which]);
				clients[i].setMultiplier(StartClient.multiplier);
				clients[i].setNumTries(StartClient.tries);
				clients[i].setTimeOut(StartClient.timeout);
			}
		}

		long time; // lol
		for (int i = 0; i < amountToSend; i++) {
			byte[] key = keygen();
			byte[] val = valgen();
			bytesSent += Code.KEY_LENGTH + Code.VALUE_LENGTH_LENGTH
					+ val.length;

			client = clients[r.nextInt(clients.length)];
			// see if put is working
			time = System.currentTimeMillis();
			boolean success = client.put(key, val);
			time = System.currentTimeMillis() - time;
			if (success) {
				minP = minP > time ? time : minP;
				maxP = maxP < time ? time : maxP;
				get += time;
			}
			if (!success) {
				System.out.println("put error!");
				p++;
				continue;
			}

			// client = clients[r.nextInt(clients.length)];
			// see if the returned value is null
			time = System.currentTimeMillis();
			byte[] retVal = client.get(key);
			time = System.currentTimeMillis() - time;
			if (retVal != null) {
				minG = minG > time ? time : minG;
				maxG = maxG < time ? time : maxG;
				put += time;
			}
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

			// check if remove worked
			time = System.currentTimeMillis();
			success = client.remove(key);
			time = System.currentTimeMillis() - time;
			if (success) {
				minR = minR > time ? time : minR;
				maxR = maxR < time ? time : maxR;
				remove += time;
			}
			if (!success) {
				System.out.println("remove error!");
				rem++;
				continue;
			}
		}
		client.close();

		ab.addAndGet(bytesSent);
		ap.addAndGet(p);
		an.addAndGet(n);
		al.addAndGet(l);
		av.addAndGet(v);
		ar.addAndGet(rem);

		putTime.addAndGet((int) put);
		getTime.addAndGet((int) get);
		removeTime.addAndGet((int) remove);

		synchronized (StartClient.minMaxLock) {
			StartClient.minR = StartClient.minR > minR ? minR
					: StartClient.minR;
			StartClient.maxR = StartClient.maxR < maxR ? maxR
					: StartClient.maxR;

			StartClient.minG = StartClient.minG > minG ? minG
					: StartClient.minG;
			StartClient.maxG = StartClient.maxG < maxG ? maxG
					: StartClient.maxG;

			StartClient.minP = StartClient.minP > minP ? minP
					: StartClient.minP;
			StartClient.maxP = StartClient.maxP < maxP ? maxP
					: StartClient.maxP;
		}
	}

	private byte[] keygen() {
		byte[] key = new byte[Code.KEY_LENGTH];
		r.nextBytes(key);
		return key;
	}

	private byte[] valgen() {
		byte[] val = new byte[r.nextInt(StartClient.maxValueLength) + 1];
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