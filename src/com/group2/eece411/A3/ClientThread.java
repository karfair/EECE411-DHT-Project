package com.group2.eece411.A3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.DatatypeConverter;

import com.group2.eece411.Config.Code;
import com.group2.eece411.DHT;
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
	
	private int clientNum;
	private ClientType c;
	
	public ClientThread(int amountToSend, String host, AtomicInteger bytesSent,
			AtomicInteger p, AtomicInteger n, AtomicInteger l, AtomicInteger v,
			AtomicInteger r, InetAddress[] selection, int[] port,
			AtomicInteger putTime, AtomicInteger getTime,
			AtomicInteger removeTime, int clientNum, ClientType c) {
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
		
		this.clientNum = clientNum;
		this.c = c;
	}

	@Override
	public void run() {
		File f;
		PrintWriter out = null;
		switch (c) {
		case PUT:
		case PUTGET:
			f = new File("result" + clientNum);
			try {
				out = new PrintWriter(new FileOutputStream(f, false));
			} catch (FileNotFoundException e) {
				System.err.println("ERR: client" + clientNum + " cannot write to file!");
				e.printStackTrace();
			}
		default:
			break;
		}
		
		BufferedReader in = null;
		switch (c) {
		case GET:
		case REMOVE:
		case GETREMOVE:
			f = new File("result" + clientNum);
			try {
				in = new BufferedReader(new FileReader(f));
			} catch (FileNotFoundException e) {
				System.err.println("ERR: client" + clientNum + " file not found!");
				e.printStackTrace();
				System.exit(1);
			}
		default:
			break;
		}
		
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
			client = clients[r.nextInt(clients.length)];
			// init values
			byte[] key = null, val = null;
			switch (c) {
			case ALL:
			case PUT:
			case PUTGET:
			case PUTREMOVE:
				key = keygen();
				val = valgen();
				break;
			case GET:
			case GETREMOVE:
			case REMOVE:
				try {
					key = DatatypeConverter.parseBase64Binary(in.readLine());
					val = DatatypeConverter.parseBase64Binary(in.readLine());
				} catch (IOException e) {
					System.err.println("ERROR: unable to readline");
					e.printStackTrace();
				}
				break;
			}
			
			switch (c) {
			case PUT:
			case PUTGET:
				// write data to file
				out.println(DatatypeConverter.printBase64Binary(key));
				out.println(DatatypeConverter.printBase64Binary(val));
			case ALL:
			case PUTREMOVE:
				// see if put is working
				time = System.currentTimeMillis();
				boolean success = client.put(key, val);
				time = System.currentTimeMillis() - time;
				if (success) {
					bytesSent += Code.KEY_LENGTH + Code.VALUE_LENGTH_LENGTH
							+ val.length;
					
					time %= StartClient.timeout;
					minP = minP > time ? time : minP;
					maxP = maxP < time ? time : maxP;
					put += time;
				}
				if (!success) {
					System.out.println("put error! val size:" + val.length);
					String keyString = DHT.positiveBigIntegerHash(key).toString();
					System.out.println(keyString.substring(0, 5) + "..." + keyString.charAt(keyString.length() - 1) + " " + keyString.length());
					p++;
					continue;
				}
			default:
				break;
			}
			
			switch (c) {
			case GET:
			case ALL:
			case PUTGET:
			case GETREMOVE:
				// client = clients[r.nextInt(clients.length)];
				// see if the returned value is null
				time = System.currentTimeMillis();
				byte[] retVal = client.get(key);
				time = System.currentTimeMillis() - time;
				if (retVal != null) {
					time %= StartClient.timeout;
					minG = minG > time ? time : minG;
					maxG = maxG < time ? time : maxG;
					get += time;
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
			default:
				break;
			}
			
			switch (c) {
			case ALL:
			case REMOVE:
			case GETREMOVE:
			case PUTREMOVE:
				// check if remove worked
				boolean success;
				time = System.currentTimeMillis();
				success = client.remove(key);
				time = System.currentTimeMillis() - time;
				if (success) {
					time %= StartClient.timeout;
					minR = minR > time ? time : minR;
					maxR = maxR < time ? time : maxR;
					remove += time;
				}
				if (!success) {
					System.out.println("remove error!");
					rem++;
					continue;
				}
			default:
				break;
			}
		}
		if (out != null) {
			out.close();
		}
		if (in != null) {
			try {
				in.close();
			} catch (IOException e) {
				System.err.println("ERR: unable to close 'in' file!");
				e.printStackTrace();
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
	
	public static enum ClientType {
		PUT(1), GET(1), REMOVE(1), ALL(3), PUTGET(2), PUTREMOVE(2), GETREMOVE(2);
		
		private int packetMultiplier;
		
		private ClientType(int packetMultiplier) {
			this.packetMultiplier = packetMultiplier;
		}
		
		public int getPacketMultiplier() {
			return packetMultiplier;
		}
	}
}