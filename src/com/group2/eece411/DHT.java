package com.group2.eece411;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Timer;
import java.util.TimerTask;

// Circular DHT: Node x key range = (previous node + 1) to x
public class DHT extends Thread {
	private static boolean VERBOSE = true;
	/**
	 * InetAddress of the successor of this node
	 */
	private InetAddress[] successor = new InetAddress[2];
	private int[] successorPort = new int[2];
	private int[] successorUDPPort = new int[2];
	private Object successorLock = new Object();

	/**
	 * This node stores all keys from startKey to endKey. Notice that endKey =
	 * thisNodeIPHash
	 */
	private BigInteger startKey;
	private Object startKeyLock = new Object();
	private BigInteger endKey; // also the this node's IP hash

	final private static BigInteger MAX_KEY = new BigInteger(new byte[] { 0,
			-128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128,
			-128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128,
			-128, -128, -128, -128, -128, -128, -128, -128, -128, -128 });

	private InetAddress thisNode;
	private ServerSocket serverSocket;
	private int thisUDPPort;

	private Timer successorChecker = new Timer();
	private boolean running = true;

	// used for setting up
	private boolean initialNode;
	private String initialNodeName;
	private int initialNodePort;

	/**
	 * Creates an Object which keeps track of the membership of the DHT
	 * 
	 * @param initialNode
	 *            - is this the first node for this DHT?
	 * @param initialNodeName
	 *            - ignored if this is the first node, otherwise this Object
	 *            will contact this node for DHT membership information
	 * @param port
	 *            - port of the above
	 */
	public DHT(boolean initialNode, String initialNodeName, int port,
			int UDPPort) {

		try {
			this.thisNode = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			System.err.println("DHT: unable to get host's IP address!");
			e.printStackTrace();
			System.exit(1);
		}

		endKey = positiveBigIntegerHash(thisNode.getAddress());

		this.initialNode = initialNode;
		this.initialNodeName = initialNodeName;
		this.initialNodePort = port;
		this.thisUDPPort = UDPPort;

		try {
			if (initialNode) {
				serverSocket = new ServerSocket(7775);
			} else {
				serverSocket = new ServerSocket(0); // bind to a random port
			}
		} catch (IOException e) {
			System.err.println("DHT: Unable to create server socket!");
			e.printStackTrace();
			System.exit(1);
		}
	}

	public int getLocalPort() {
		return serverSocket.getLocalPort();
	}

	public InetAddress getLocalHost() {
		return thisNode;
	}

	/**
	 * Stops this thread safety and also does a join()
	 */
	public void stopMe() {
		successorChecker.cancel();
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e1) {
			System.err.println("DHT closing socket failed!");
			e1.printStackTrace();
		}
		try {
			join();
		} catch (InterruptedException e) {
			System.err.println("DHT.join() failed!");
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		try {
			if (!initialNode) {
				sendInitialJoinRequest();
			} else {
				startKey = circularPlusOne(endKey);
			}
			while (running) {
				Socket clientSocket = serverSocket.accept();

				BufferedReader in = new BufferedReader(new InputStreamReader(
						clientSocket.getInputStream()));
				PrintWriter out = new PrintWriter(
						clientSocket.getOutputStream(), true);

				try {
					String input;
					input = in.readLine();
					if (VERBOSE) {
						System.out.println(input + " @ "
								+ thisNode.getHostAddress() + ":"
								+ serverSocket.getLocalPort());
						if (startKey != null && endKey != null) {
							System.out.println("s" + startKey.toString());
							System.out.println("e" + endKey.toString());
						}
					}
					if (input.equals("join")) {
						String node = in.readLine();
						int port = Integer.parseInt(in.readLine());
						int udpPort = Integer.parseInt(in.readLine());
						if (VERBOSE) {
							System.out.println("node: " + node + " port: "
									+ port + " udpPort: " + udpPort);
						}
						processJoinRequest(node, port, udpPort);
					} else if (input.equals("done")) {
						String successor = in.readLine();
						int port = Integer.parseInt(in.readLine());
						int udpPort = Integer.parseInt(in.readLine());
						String startKey = in.readLine();
						done(successor, startKey, port, udpPort);
					} else if (input.equals("fail")) {
						BigInteger startKey = getStartKey(in.readLine());
						synchronized (startKeyLock) {
							this.startKey = startKey;
						}
					} else if (input.equals("getSuccessor")) {
						sendFirstSuccessor(out);
					} else if (input.equals("alive")) {
						out.println("yes");
					} else if (input.equals("update")) {
						String newNode = in.readLine();
						String oldNode = in.readLine();
						int newPort = Integer.parseInt(in.readLine());
						int udpPort = Integer.parseInt(in.readLine());
						processUpdate(newNode, oldNode, newPort, udpPort);
					}
					clientSocket.close();

				} catch (NullPointerException npe) {
					// probably means it is communicating to has died
					// or the dht is shutting down
					// TODO - it cant die if what's it;s currently taljign to is
					// dead
					if (!serverSocket.isClosed()) {
						System.err.println("DHT server failed! -> NPE");
						npe.printStackTrace();
					}
				}
			}
		} catch (IOException e) {
			if (!serverSocket.isClosed()) {
				System.err.println("DHT server failed! -> IOException");
				e.printStackTrace();
			}
		}
	}

	/**
	 * Produce a positive big Interger hash of the data
	 * 
	 * @param data
	 *            - to be hashed, Big Endian format
	 * @return
	 */
	private static BigInteger positiveBigIntegerHash(byte[] data) {
		byte[] hash = hash(data);
		return getPositiveBigInteger(hash);
	}

	public Successor getFirstSuccessor() {
		InetAddress s;
		int udpport;
		synchronized (successorLock) {
			s = successor[0];
			udpport = successorUDPPort[0];
		}
		return new Successor(s, udpport);
	}

	private void sendFirstSuccessor(PrintWriter out) {
		String successorName = null;
		int port = 0;
		int udpPort = 0;
		boolean isNull = true;
		synchronized (successorLock) {
			if (successor[0] != null) {
				isNull = false;
				successorName = successor[0].getHostAddress();
				port = successorPort[0];
				udpPort = successorUDPPort[0];
			}
		}
		// if the variable are not initialized, then send ""
		if (isNull) {
			out.println("");
			out.println("");
			out.println("");
		} else {
			out.println(successorName);
			out.println(port);
			out.println(udpPort);
		}
	}

	static private InetAddress getByName(String name) {
		InetAddress inetNode = null;
		try {
			inetNode = InetAddress.getByName(name);
		} catch (UnknownHostException e) {
			System.err.println("DHT.getByName() failed! Cannot resolve name.");
			e.printStackTrace();
		}
		return inetNode;
	}

	private void processJoinRequest(String node, int port, int udpPort) {
		InetAddress inetNode = getByName(node);

		if (isKeyInRange(inetNode.getAddress())) {
			if (VERBOSE) {
				System.out.println("accepting join");
			}
			Socket clientSocket = null;
			String stringStartKey;
			synchronized (startKeyLock) {
				stringStartKey = startKey.toString();
				startKey = getStartKey(node);
			}
			try {
				clientSocket = new Socket(inetNode, port);

				PrintWriter out = new PrintWriter(
						clientSocket.getOutputStream(), true);

				out.println("done");
				out.println(thisNode.getHostAddress());
				out.println(serverSocket.getLocalPort());
				out.println(thisUDPPort);
				out.println(stringStartKey);

				clientSocket.close();
			} catch (IOException e) {
				System.err.println("DHT.processJoinRequest() failed!");
				e.printStackTrace();
			}
			if (initialNode == true) {
				initialNode = false;
				successor[0] = inetNode;
				successorPort[0] = port;
				successorUDPPort[0] = udpPort;
				startCheckSuccessor();
			} else {
				update(inetNode.getHostAddress(), thisNode.getHostAddress(),
						port, udpPort);
			}
		} else {
			if (VERBOSE) {
				System.out.println("passing join");
			}
			passJoin(inetNode, port, udpPort);
		}
	}

	private void processUpdate(String newNode, String oldNode, int newPort,
			int udpPort) {
		InetAddress inetNewNode = getByName(newNode);
		if (oldNode.equals(thisNode.getHostAddress())) {
			return;
		}

		if (!newNode.equals(thisNode.getHostAddress())) {
			synchronized (successorLock) {
				if (oldNode.equals(successor[0].getHostAddress())) {
					successor[1] = successor[0];
					successorPort[1] = successorPort[0];
					successorUDPPort[1] = successorUDPPort[0];

					successor[0] = inetNewNode;
					successorPort[0] = newPort;
					successorUDPPort[0] = udpPort;
				} else if (oldNode.equals(successor[1].getHostAddress())) {
					successor[1] = inetNewNode;
					successorPort[1] = newPort;
					successorUDPPort[1] = udpPort;
				}
			}
		}
		update(newNode, oldNode, newPort, udpPort);
	}

	private void update(String newNode, String oldNode, int newPort, int udpPort) {
		Socket clientSocket = null;
		try {
			InetAddress s;
			int port;
			synchronized (successorLock) {
				s = successor[0];
				port = successorPort[0];
			}
			clientSocket = new Socket(s, port);

			PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),
					true);
			if (VERBOSE) {
				System.out.println("update called: newNode:" + newNode
						+ " oldNode:" + oldNode + " newPort:" + newPort);
			}

			out.println("update");
			out.println(newNode);
			out.println(oldNode);
			out.println(newPort);
			out.println(udpPort);

			clientSocket.close();

		} catch (IOException e) {
			System.err.println("DHT.update() failed!");
			e.printStackTrace();
		}
	}

	private static byte[] hash(byte[] data) {
		// if (VERBOSE) {
		// System.out.println("hashing: "
		// + DatatypeConverter.printHexBinary(data) + " numbytes: "
		// + data.length);
		// }
		MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			// should not happen
			System.err.println("DHT.hash() failed! SHA-256 not found!");
			e.printStackTrace();
			return new byte[32];
		}
		messageDigest.update(data);
		return messageDigest.digest();
	}

	private void sendInitialJoinRequest() {
		Socket clientSocket = null;
		try {
			clientSocket = new Socket(initialNodeName, initialNodePort);

			PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),
					true);

			out.println("join");
			out.println(thisNode.getHostAddress());
			out.println(serverSocket.getLocalPort());
			out.println(thisUDPPort);

			clientSocket.close();
		} catch (IOException e) {
			System.err.println("DHT.sendInitialJoinRequest() failed!");
			e.printStackTrace();
		}
	}

	private void passJoin(InetAddress inetNode, int port, int udpPort) {
		Socket clientSocket = null;
		try {
			InetAddress s;
			int p;
			synchronized (successorLock) {
				s = successor[0];
				p = successorPort[0];
			}
			clientSocket = new Socket(s, p);

			PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),
					true);

			out.println("join");
			out.println(inetNode.getHostAddress());
			out.println(port);
			out.println(udpPort);

			clientSocket.close();
		} catch (IOException e) {
			System.err.println("DHT.passJoin() failed!");
			e.printStackTrace();
		}
	}

	private void done(String successor, String startKey, int port, int udpPort) {
		if (VERBOSE) {
			System.out.println("done is called!");
			System.out.println("successor:" + successor);
		}
		InetAddress s = getByName(successor);

		synchronized (successorLock) {
			this.successor[0] = s;
			this.successorPort[0] = port;
			this.successorUDPPort[0] = udpPort;
		}
		this.getAndSetSuccessorFrom(s, port, 1);

		synchronized (startKeyLock) {
			this.startKey = new BigInteger(startKey);
		}

		startCheckSuccessor();
	}

	private void startCheckSuccessor() {
		successorChecker.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				checkSuccessor();
			}
		}, 10000, 10000);
	}

	private static BigInteger getStartKey(String predecessor) {

		InetAddress p = getByName(predecessor);

		BigInteger startKey = positiveBigIntegerHash(p.getAddress());

		return circularPlusOne(startKey);
	}

	private static BigInteger circularPlusOne(BigInteger i) {
		if (i.compareTo(MAX_KEY) == 0) { // on the 1 in kagillion chance
			// that this might happen
			return new BigInteger(new byte[33]);
		} else {
			return i.add(BigInteger.ONE);
		}
	}

	private void checkSuccessor() {
		Socket clientSocket = null;
		try {
			InetAddress s;
			int p;
			synchronized (successorLock) {
				s = successor[0];
				p = successorPort[0];
			}
			if (VERBOSE) {
				System.out.println("checkSuccessor() on " + s.getHostAddress()
						+ ":" + p);
			}

			clientSocket = new Socket(s, p);

			BufferedReader in = new BufferedReader(new InputStreamReader(
					clientSocket.getInputStream()));
			PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),
					true);

			out.println("alive");
			String reply = in.readLine();
			if (reply == null) {
				// probably mean who this guy is talking to has died
				// TODO:
				return;
			}
			if (!reply.equals("yes")) {
				System.err
						.println("Server is alive but it's not replying \"yes\"...");
				System.err.println("it's replying: " + reply);
			}

			clientSocket.close();

			// cannot connect to successor, assume it is dead
			// it should try again before contacting 1 TODO
		} catch (IOException e) {
			try {
				InetAddress s;
				int p;
				synchronized (successorLock) {
					s = successor[1];
					p = successorPort[1];
				}
				if (VERBOSE) {
					System.out.println("Node failure occurred!");
					System.out.println("Will try to connect to "
							+ s.getHostAddress() + ":" + p);

				}
				clientSocket = new Socket(s, p);

				PrintWriter out = new PrintWriter(
						clientSocket.getOutputStream(), true);

				out.println("fail");
				out.println(thisNode.getHostAddress());

				clientSocket.close();
			} catch (IOException e1) {
				System.err
						.println("DHT.checkSuccessor() failed! Both successor died :(");
				// both successor died
				e1.printStackTrace();
				System.exit(1);
			}
			InetAddress s;
			int p;
			synchronized (successorLock) {
				s = successor[1];
				p = successorPort[1];
				successor[0] = successor[1];
				successorPort[0] = successorPort[1];
			}

			getAndSetSuccessorFrom(s, p, 1);
		}
	}

	private void getAndSetSuccessorFrom(InetAddress here, int port, int which) {
		Socket clientSocket = null;
		String reply = null;
		String returnPort = null;
		try {
			clientSocket = new Socket(here, port);

			BufferedReader in = new BufferedReader(new InputStreamReader(
					clientSocket.getInputStream()));
			PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),
					true);

			out.println("getSuccessor");
			reply = in.readLine();
			returnPort = in.readLine();

			clientSocket.close();
		} catch (IOException e) {
			System.err.println("DHT.getSuccessorFrom() failed!");
			e.printStackTrace();
		}
		if (reply == null || returnPort == null) {
			// probably mean who this guy is talking to has died
			// TODO:
			return;
		}
		if (reply.equals("") || returnPort.equals("")) {
			return;
		}
		synchronized (successorLock) {
			this.successor[which] = getByName(reply);
			this.successorPort[which] = Integer.parseInt(returnPort);
		}
	}

	static private BigInteger getPositiveBigInteger(byte[] data) {
		if (data[0] != 0) {
			byte[] unsignedData;
			unsignedData = new byte[data.length + 1];
			System.arraycopy(data, 0, unsignedData, 1, data.length);
			return new BigInteger(unsignedData);
		} else {
			return new BigInteger(data);
		}
	}

	/**
	 * Checks if the key should be stored at this node
	 * 
	 * @param key
	 *            - the key to be stored
	 * @return true if the key should be stored at this node, false otherwise
	 */
	public boolean isKeyInRange(byte[] key) {
		BigInteger wrapKey = positiveBigIntegerHash(key);

		synchronized (startKeyLock) {
			if (endKey.compareTo(startKey) > 0) {
				if (wrapKey.compareTo(startKey) >= 0
						&& wrapKey.compareTo(endKey) <= 0) {
					return true;
				}
				return false;
			} else {
				if (wrapKey.compareTo(startKey) >= 0
						|| wrapKey.compareTo(endKey) <= 0) {
					return true;
				}
				return false;
			}
		}
	}

	public class Successor {
		public int udpPort;
		public InetAddress ip;

		public Successor(InetAddress ip, int udpPort) {
			this.udpPort = udpPort;
			this.ip = ip;
		}
	}
}