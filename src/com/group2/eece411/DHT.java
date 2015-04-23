package com.group2.eece411;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

// Circular DHT: Node x key range = (previous node + 1) to x
public class DHT extends Thread {

	public static final int DEFAULT_DHT_TCP_PORT = 7775;

	private static boolean VERBOSE = false;
	private static boolean LESS_VERBOSE = false;
	/**
	 * InetAddress of the successor of this node
	 */
	private ArrayList<Successor> successor = new ArrayList<Successor>();
	private int maxSuccessor = 7;
	// private InetAddress[] successor = new InetAddress[2];
	// private int[] successorPort = new int[2];
	// private int[] successorUDPPort = new int[2];
	private Object successorLock = new Object();
	// private boolean[] isDead = new boolean[2];

	/**
	 * This node stores all keys from startKey to endKey. Notice that endKey =
	 * thisNodeIPHash
	 */
	private BigInteger startKey;
	private Object startKeyLock = new Object();
	private BigInteger endKey; // also the this node's IP hash

	final private static BigInteger MAX_KEY = new BigInteger(new byte[] { 0,
			-128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128,
			-128, -128, -128, -128, -128 });
			//, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128 });

	private InetAddress thisNode;
	private ServerSocket serverSocket;
	private int thisUDPPort;

	private Timer successorChecker = new Timer();
	private boolean running = true;

	// used for setting up
	private boolean initialNode;
	private String initialNodeName;
	private int initialNodePort;

	// sort successor lock
	private Semaphore sort = new Semaphore(1);
	
	private Semaphore darudeSendLock = new Semaphore(1);
	
	//
	private List<Darude> allNodes = new ArrayList<Darude>();
	private Timer tokenChecker = new Timer();
	private AtomicLong lastTokenReceived = new AtomicLong(System.currentTimeMillis());
	
	private static final boolean TOKEN_VERBOSE = false;
	
	private ArrayList<InetAddress> tempNodeList = new ArrayList<InetAddress>();

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
	 * @param UDPPort
	 *            - client port
	 */
	public DHT(boolean initialNode, String initialNodeName, int port,
			int UDPPort, String nodeList) {

		try {
			this.thisNode = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			System.err.println("DHT: unable to get host's IP address!");
			e.printStackTrace();
			System.exit(1);
		}

		if (VERBOSE) {
			System.out.println("thisIP:" + thisNode.getHostAddress());
		}

		endKey = positiveBigIntegerHash(thisNode.getAddress());

		this.initialNode = initialNode;
		this.initialNodeName = initialNodeName;
		this.initialNodePort = port;
		this.thisUDPPort = UDPPort;

		try {
			if (initialNode) {
				serverSocket = new ServerSocket(DEFAULT_DHT_TCP_PORT);
				// bind to default port
			} else {
				serverSocket = new ServerSocket(DEFAULT_DHT_TCP_PORT);
				// changed to a default port to facilitate joining

			}
		} catch (IOException e) {
			System.err.println("DHT: Unable to create server socket!");
			e.printStackTrace();
			System.exit(1);
		}
		
		if (nodeList != null) {
			// create a successor list from file
			File f = new File(nodeList);
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(f));
			} catch (FileNotFoundException e) {
				System.err.println("ERR: nodelist not found!");
				e.printStackTrace();
				System.exit(1);
			}
			
			// load it into an inet list
			String line;
			List<EndKeyWithAddress> list = new ArrayList<EndKeyWithAddress>();
			try {
				while ((line = in.readLine()) != null) {
					list.add(new EndKeyWithAddress(InetAddress.getByName(line)));
				}
			} catch (IOException e) {
				System.err.println("ERR: unable to read nodelist!");
				e.printStackTrace();
				System.exit(1);
			}
			Collections.sort(list);
			
			// find 3 successors
			int index;
			for (index = 0; index < list.size(); index++) {
				if (list.get(index).endKey.equals(endKey)) {
					break;
				}
			}

			for (int i = 1; i <= maxSuccessor; i++) {
				tempNodeList.add(list.get((index + i) % list.size()).addr);
			}
			
			if (TOKEN_VERBOSE) {
				System.out.println("This node:" + thisNode.getHostAddress());
				System.out.println("This node:" + list.get(index).addr.getHostAddress());
				System.out.println("successor1:" + tempNodeList.get(0).getHostAddress());
				System.out.println("successor2:" + tempNodeList.get(1).getHostAddress());
				System.out.println("successor3:" + tempNodeList.get(2).getHostAddress());
				
				String ip = "";
				String hash = "";
				for (EndKeyWithAddress d : list) {
					
					String key = d.endKey.toString();
					String partialKey = key.substring(0, 5) + "..." + key.charAt(key.length() - 1) + " " + key.length();
					
					ip += d.addr.getHostAddress() + "\n";
					hash += partialKey + "\n";
				}
				System.out.println(ip);
				System.out.println(hash);
			}
			
			// set initial node to false
			this.initialNode = false;
		}
	}
	
	private static class EndKeyWithAddress implements Comparable<EndKeyWithAddress> {
		public final BigInteger endKey;
		public final InetAddress addr;
		
		public EndKeyWithAddress(InetAddress addr) {
			this.addr = addr;
			endKey = positiveBigIntegerHash(addr.getAddress());
		}

		@Override
		public int compareTo(EndKeyWithAddress o) {
			return endKey.compareTo(o.endKey);
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
		if (!initialNode) {
			if (tempNodeList.size() != 0) {
				startKey = circularPlusOne(endKey);
				new Thread() {
					public void run() {
						try {
							Thread.sleep(20000);
							for (InetAddress i : tempNodeList) {
								Successor su;
								try {
									su = new Successor(i, DEFAULT_DHT_TCP_PORT, Config.validPort[0]);
									successor.add(su);
								} catch (IOException e) {
									System.err.println("unable to contact node");
									e.printStackTrace();
								}
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

						startCheckSuccessor();
					}
				}.start();
			} else {
				sendInitialJoinRequest();
			}
		} else {
			startKey = circularPlusOne(endKey);
		}
		while (running) {
			try {
				Socket clientSocket = serverSocket.accept();
				new InSocket(clientSocket).start();
			} catch (IOException e) {
				System.err.println("ERR: unable to accept socket!");
				e.printStackTrace();
			}
		}
	}
	
	private static String readLine(InputStream is) throws IOException {
		String input = "";
		int c;
		while ((c = is.read()) != '\n') {
			if (c == -1) {
				throw new IOException("socket closed!");
			}
			input += (char) c;
		}
		input = input.replace("\r", "");
		return input;
	}
	
	public class InSocket extends Thread {

		private final Socket clientSocket;
		
		public InSocket(Socket clientSocket) {
			super("PersistentSocket");
			this.clientSocket = clientSocket;
		}

		private String readLine() throws IOException {
			return DHT.readLine(clientSocket.getInputStream());
		}
		
		@Override
		public void run() {
			try {
				while (true) {
					String input = readLine();
					if (input.contains("token")) {
						if (TOKEN_VERBOSE) {
							System.out.println("getting token");
						}
						new DarudeSandstorm(clientSocket).run();
					} else {
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
							String node = readLine();
							int port = Integer.parseInt(readLine());
							int udpPort = Integer.parseInt(readLine());
							if (VERBOSE) {
								System.out.println("node: " + node + " port: "
										+ port + " udpPort: " + udpPort);
							}
							processJoinRequest(node, port, udpPort);
						} else if (input.equals("done")) {
							String successor = readLine();
							int port = Integer.parseInt(readLine());
							int udpPort = Integer.parseInt(readLine());
							String startKey = readLine();
							done(successor, startKey, port, udpPort);
						} else if (input.equals("fail")) {
							BigInteger startKey = getStartKey(readLine());
							synchronized (startKeyLock) {
								if (startKey.compareTo(DHT.this.startKey) != 0) {
									DHT.this.startKey = startKey;
								}
							}
						} else if (input.equals("getSuccessor")) {
							PrintWriter out = new PrintWriter(
									clientSocket.getOutputStream(), true);
							sendAllSuccessor(out);
						} else if (input.equals("alive")) {
							new PrintWriter(clientSocket.getOutputStream(), true).println("yes");
						} else if (input.equals("update")) {
							String newNode = readLine();
							String oldNode = readLine();
							int newPort = Integer.parseInt(readLine());
							int udpPort = Integer.parseInt(readLine());
							processUpdate(newNode, oldNode, newPort, udpPort);
						}
					}
				}
			} catch (NullPointerException npe) {
				// probably means it is communicating to has died
				// or the dht is shutting down
				System.err.println(thisNode.getCanonicalHostName());
				System.err.println("DHT server failed! -> NPE");
				npe.printStackTrace();

			} catch (IOException e) {
				// probably means it is communicating to has died
				// or the dht is shutting down

				System.err.println("DHT server failed! -> IOException");
				e.printStackTrace();

			} catch (Exception e) {
				System.err
						.println("DHT server failed! -> something really weird...");
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Stores an InetAddress and its corresponding hashed value
	 * @author Phil
	 *
	 */
	public static class Darude implements Comparable<Object> {
		public final InetAddress addr;
		public final BigInteger endKey;
		public BigInteger startKey;
		
		private final boolean endKeyGreaterThanStart;

		public Darude(InetAddress addr, BigInteger endKey, BigInteger startKey) {
			this.addr = addr;
			this.endKey = endKey;
			this.startKey = startKey;
			
			if (endKey.compareTo(startKey) > 0) {
				endKeyGreaterThanStart = true;
			} else {
				endKeyGreaterThanStart = false;
			}
		}
		
		@Override
		public int compareTo(Object o) {
			BigInteger that;
			if (o instanceof Darude) {
				that = ((Darude) o).endKey;
			} else {
				that = (BigInteger) o;
			} 	
			
			if (endKeyGreaterThanStart) {
				if (startKey.compareTo(that) <= 0) {
					if (endKey.compareTo(that) >= 0) {
						return 0;
					} else {
						return -1;
					}
				} else /* startKey > that */ {
					return 1;
				}
			} else {
				// this case only ever applies to the first element of the list
				// if it isnt found here, that means what you're looking for is greater than this #
				if (startKey.compareTo(that) <= 0 || endKey.compareTo(that) >= 0) {
					return 0;
				} else {
					return -1;
				}
			}
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof Darude) {
				Darude d = (Darude) o;
				return endKey.equals(d.endKey);
			} else if (o instanceof BigInteger) {
				BigInteger key = (BigInteger) o;
				if (endKeyGreaterThanStart) {
					if (key.compareTo(startKey) >= 0
							&& key.compareTo(endKey) <= 0) {
						return true;
					}
					return false;
				} else {
					if (key.compareTo(startKey) >= 0
							|| key.compareTo(endKey) <= 0) {
						return true;
					}
					return false;
				}
			} else {
				return false;
			}
		}
	}
	
	public class DarudeSandstorm implements Runnable {
		Socket clientSocket;
		ObjectInputStream ois;
		List<InetAddress> list;
		
		public DarudeSandstorm(Socket clientSocket) throws IOException {
			this.clientSocket = clientSocket;
			ois = new ObjectInputStream(clientSocket.getInputStream());
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			// gets the list of all participating nodes
			try {
				list = (List<InetAddress>) ois.readObject();
			} catch (ClassNotFoundException | IOException e) {
				System.err.println("DHT server failed! -> error reading token");
				e.printStackTrace();
				return;
			}
			
			if (darudeSendLock.tryAcquire()) {
				// TODO
				long roundTrip = System.currentTimeMillis() - lastTokenReceived.get();
				if (roundTrip < 5000 && list.size() > 50) {
					darudeSendLock.release();
					return;
				}
				if (TOKEN_VERBOSE) {
					System.out.println("Token round trip: " + ((System.currentTimeMillis() - lastTokenReceived.get()) / 1000.0) + "s.");
				}
				
				lastTokenReceived.set(System.currentTimeMillis());
				darude();
				
				// forwards the token
				if (TOKEN_VERBOSE) {
					System.out.println("forwarding token");
				}
				
				if (!list.isEmpty()) {
					forward(list, "token");
				}
				
				darudeSendLock.release();
			}
		}
		
		/**
		 * Weed out IMMEDIATE dead successor.
		 */
		private void darude() {
			// get index of this node
			Successor first = getFirstSuccessor();
			
			int i, thisIndex = -1, successorIndex = -1;
			for (i = 0; i < list.size(); i++) {
				if (list.get(i).equals(thisNode)) {
					thisIndex = i;
				}
				if (list.get(i).equals(first.ip)) {
					successorIndex = i;
				}
			}
			
			int thisIndexCircularPlusOne = list.size() == 0 ? -2 : (thisIndex + 1) % list.size();
			if (thisIndex == -1) {
				// means the list is not yet completed, add yourself to it
				list.add(thisNode);
				return;
			} else if (successorIndex == -1) {
				// successor not found, means this successor is new so
				// add the successor to the list
				if (positiveBigIntegerHash(first.ip.getAddress()).compareTo(endKey) < 0) {
					// successor is the lowest numbered one
					list.add(0, first.ip);
				} else {
					// successor is right after this one
					list.add(thisIndex + 1, first.ip);
				}
			} else if (thisIndexCircularPlusOne == successorIndex) {
				// do nothing as its successor is unchanged
			} else {
				// there are some immediate dead successor, remove them
				
				// normal remove, successor index is after this index
				if (thisIndexCircularPlusOne < successorIndex) {
					int removeAmount = successorIndex - thisIndexCircularPlusOne;
					System.out.println(thisNode.getHostAddress() + "\nremoving amoutn:" + removeAmount );
					System.out.println("size before: " + list.size());
					for (int k = 0; k < removeAmount; k++) {
						list.remove(thisIndexCircularPlusOne);
					}
					System.out.println("size after: " + list.size());
				// abnormal remove, successor index is before this one
				} else {
					try {
						while (true) {
							list.remove(thisIndexCircularPlusOne);
						}
					} catch (IndexOutOfBoundsException e) {
						// done
					}
					
					while(!list.get(0).equals(first.ip)) {
						list.remove(0);
					}
				}
			}
			
			sandstorm();
		}
		
		/**
		 * Changes this node's allNode list from the new received token
		 */
		private void sandstorm() {
			InetAddress lastNode = list.get(list.size() - 1);
			byte[] lastNodeAddress = lastNode.getAddress();
			BigInteger previousEndKeyPlusOne = circularPlusOne(positiveBigIntegerHash(lastNodeAddress));
			BigInteger currentEndKey;
			List<Darude> l = new ArrayList<Darude>();
			
			BigInteger prev = BigInteger.ZERO;
			for (InetAddress addr : list) {
				currentEndKey = positiveBigIntegerHash(addr.getAddress());
				
				// check if the list if sorted, if not drop the list and don't pass it on
				if (currentEndKey.compareTo(prev) <= 0) {
					if (TOKEN_VERBOSE) {
						System.out.println("dropping token... list order messed up");
					}
					list.clear();
					return;
				} else {
					prev = currentEndKey;
				}
				Darude d = new Darude(addr, currentEndKey, previousEndKeyPlusOne);
				previousEndKeyPlusOne = circularPlusOne(currentEndKey);
				
				l.add(d);
			}
			
			// Collections.sort(l);
			allNodes = l;
		}
	}
	
	private void checkToken() {
		//if (initialNode) {
		//	resetStates();
		//	return;
		//}
		
		boolean leader;
		BigInteger successor = positiveBigIntegerHash(getFirstSuccessor().ip.getAddress());
		if (successor.compareTo(endKey) < 0) {
			leader = true;
		} else {
			leader = false;
		}
		
		//if (thisNode.getHostAddress().equals("142.103.2.1")) {
		//TODO
		if (leader) {
			long waited = System.currentTimeMillis() - lastTokenReceived.get();
			long limit;
			if (allNodes.size() > 100) {
				limit = 90000;
			} else {
				limit = 60000;
			}
			if (waited > limit) {
				if (TOKEN_VERBOSE) {
					System.out.println("generating token");
				}
				lastTokenReceived.set(System.currentTimeMillis());
				forward(new ArrayList<InetAddress>(), "token");
			}
		}
		
		if (TOKEN_VERBOSE) {
			List<Darude> allNodes = this.allNodes;
			
			String ip = "";
			String hash = "";
			for (Darude d : allNodes) {
				String key = d.startKey.toString();
				String partialKey = key.substring(0, 5) + "..." + key.charAt(key.length() - 1) + " " + key.length();
				
				key = d.endKey.toString();
				partialKey += " | " + key.substring(0, 5) + "..." + key.charAt(key.length() - 1) + " " + key.length();
				
				ip += d.addr.getHostAddress() + "\n";
				hash += partialKey + "\n";
			}
			System.out.println("size of list:" + allNodes.size());
			System.out.println(ip);
			System.out.println(hash);
		}
		System.out.println("size of master list: " + allNodes.size());
	}
	

	private void forward(Object darude, String header) {
		//if (initialNode) {
		//	resetStates();
		//	return;
		//}
		
		ArrayList<Successor> copy = getCopy();

		int i;
		for (i = 0; i < copy.size(); i++) {
			Successor s = copy.get(i);
			if (!s.isAlive()) {
				if (VERBOSE) {
					System.out.println("successor " + i + " is dead.");
				}
				continue;
			}

			if (VERBOSE) {
				System.out.println("sending LIST to ip:" + s.ip.getHostAddress()
						+ "@" + s.tcpPort);
			}

			try {
				s.send(header, darude);
				break;
			} catch (IOException e) {
				// if any of the read or write operations fails, control will
				// continue here, we will log the server that had failed
				// and try the next server
				s.setDead();
				if (VERBOSE) {
					System.out.println("node " + i + " just died. msg:"
							+ e.getMessage());
					e.printStackTrace();
				}
			}
		}

		if (i == copy.size()) {
//			resetStates();
//			return;
			System.err
					.println("DHT.forward(): All successors are dead. Server shutting down...");
			System.err.println("sucessor.size(): " + copy.size() + " i: " + i);
			System.exit(1);
		}
	}

	/**
	 * Produce a positive big Interger hash of the data
	 * 
	 * @param data
	 *            - to be hashed, Big Endian format
	 * @return
	 */
	public static BigInteger positiveBigIntegerHash(byte[] data) {
		byte[] hash = hash(data);
		return getPositiveBigInteger(hash);
	}

	public Successor getFirstSuccessor() {
		Successor s = null;
		synchronized (successorLock) {
			for (Successor su : successor) {
				if (su.isAlive()) {
					s = su;
					break;
				}
			}
		}
		return s;
	}

	public Successor closestSuccessorTo(byte[] key) {
		
		List<Darude> allNodes = this.allNodes;
		// algorithm does not for under 3 nodes
		if (allNodes.size() >= 3) {
			int location = Collections.binarySearch(allNodes, positiveBigIntegerHash(key));
		
			// if not found, that means it is the highest number in the list, so 
			// the highest # is stored on the 1st node
			location = location < 0 ? 0 : location;
			
			Darude d = allNodes.get(location);
			return new Successor(d.addr);
		} else {
			return getFirstSuccessor();
		}
		/*
		
		ArrayList<Successor> a = getCopy();
		BigInteger wrapKey = positiveBigIntegerHash(key);
		BigInteger startKey = circularPlusOne(endKey);
		Successor last = null;

		for (Successor s : a) {
			if (s.isAlive()) {
				BigInteger endKey = positiveBigIntegerHash(s.ip.getAddress());
				last = s;
				if (endKey.compareTo(startKey) > 0) {
					if (wrapKey.compareTo(startKey) >= 0
							&& wrapKey.compareTo(endKey) <= 0) {
						return s;
					}
				} else {
					if (wrapKey.compareTo(startKey) >= 0
							|| wrapKey.compareTo(endKey) <= 0) {
						return s;
					}
				}
			}
		}
		return last;*/
	}

	public ArrayList<Successor> firstTwoSuccessor() {
		ArrayList<Successor> copy = getCopy();
		ArrayList<Successor> ret = new ArrayList<>();
		if (copy.isEmpty()) {
			return ret;
		}
		int count = 0;
		for (Successor s : copy) {
			if (s.isAlive()) {
				ret.add(s);
				count++;
				if (count >= 2)
					break;
			}
		}
		return ret;
	}

	// send successor, regardless of deadness or aliveness for correctness of
	// update need to be held to ensure correctness
	private void sendAllSuccessor(PrintWriter out) {

		ArrayList<Successor> copy = getCopy();

		String successorName = "";
		String port = "";
		String udpPort = "";

		for (int i = 0; i < copy.size(); i++) {
			Successor s = copy.get(i);
			if (!s.canBeRemoved()) {
				successorName += s.ip.getHostAddress() + "|";
				port += s.tcpPort + "|";
				udpPort += s.udpPort + "|";
			}
		}

		successorName = successorName.substring(0, successorName.length() - 1);
		port = port.substring(0, port.length() - 1);
		udpPort = udpPort.substring(0, udpPort.length() - 1);

		out.println(successorName);
		out.println(port);
		out.println(udpPort);
		
		out.flush();
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
				
				out.flush();
				clientSocket.close();
			} catch (IOException e) {
				System.err.println("DHT.processJoinRequest() failed!");
				e.printStackTrace();
				// above operation didn't finish, assume the server
				// it was talking to has died, so do nothing
				// should not happen ever
				return;
			}
			if (initialNode) {
				initialNode = false;
				try {
					// if initial node, anything that joins becomes successor
					Successor su = new Successor(inetNode, port, udpPort);
					successor.add(su);
					startCheckSuccessor();
				} catch (IOException e) {
					// successor is dead //again super error
					System.err.println("ERR: Successor is dead! reseting states...");
					resetStates();
					e.printStackTrace();
				}
			} else {
				// the node should update it self first
				// in case it is its own successor, but this should not happen
				// in case of small DHT ring, may need to update self
				processUpdate(inetNode.getHostAddress(), thisNode.getHostAddress(),
						port, udpPort);
				// sends update messages
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
	
	@Deprecated
	private void resetStates() {
		initialNode = true;
		successorChecker.cancel();
		tokenChecker.cancel();
		
		startKey = circularPlusOne(endKey);
		
		successor = new ArrayList<Successor>();
		allNodes = new ArrayList<Darude>();
		
		successorChecker = new Timer();
		tokenChecker = new Timer();
	}

	private void processUpdate(String newNode, String oldNode, int newPort,
			int udpPort) {
		InetAddress inetNewNode = getByName(newNode);

		if (!newNode.equals(thisNode.getHostAddress())) {
			synchronized (successorLock) {
				for (int i = 0; i < successor.size(); i++) {
					String successorIP = successor.get(i).ip.getHostAddress();
					// kill the msg, it has been passed too many times
					if (successorIP.equals(newNode)) {
						return;
					}
					if (successorIP.equals(oldNode)) {
						try {
							Successor su = new Successor(inetNewNode, newPort, udpPort);
							successor.add(i, su);
						} catch (IOException e) {
							System.err.println("ERR:cannot contact successor!");
							e.printStackTrace();
						}
						break;
					}
				}
			}
		}

		// TODO: in extreme cases, update should have a hop count
		if (oldNode.equals(thisNode.getHostAddress())) {
			return;
		}
		update(newNode, oldNode, newPort, udpPort);
	}

	/**
	 * passes the update message
	 * @param newNode
	 * @param oldNode
	 * @param newPort
	 * @param udpPort
	 */
	private void update(String newNode, String oldNode, int newPort, int udpPort) {
		if (VERBOSE) {
			System.out.println("update called: newNode:" + newNode
					+ " oldNode:" + oldNode + " newPort:" + newPort);
		}
		String[] msg = new String[] { "update", newNode, oldNode,
				String.valueOf(newPort), String.valueOf(udpPort) };
		forward(msg, 0, true, positiveBigIntegerHash(getByName(oldNode).getAddress()));
	}

	private static byte[] hash(byte[] data) {
		// if (VERBOSE) {
		// System.out.println("hashing: "
		// + DatatypeConverter.printHexBinary(data) + " numbytes: "
		// + data.length);
		// }
		MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// should not happen
			System.err.println("DHT.hash() failed! SHA-256 not found!");
			System.err.println("This should never ever happen!");
			e.printStackTrace();
			System.exit(2);
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

			out.flush();
			if (out.checkError()) {
				throw new IOException("ERR: cannot talk to another node");
			}
			clientSocket.close();
		} catch (IOException e) {
			System.err.println("DHT.sendInitialJoinRequest() failed!");
			System.err.println("Server shutting down...");
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void passJoin(InetAddress inetNode, int port, int udpPort) {
		String[] msg = new String[] { "join", inetNode.getHostAddress(),
				String.valueOf(port), String.valueOf(udpPort) };
		forward(msg, 0, true, positiveBigIntegerHash(inetNode.getAddress()));
	}

	private void done(String successor, String startKey, int port, int udpPort) {
		if (VERBOSE) {
			System.out.println("done is called!");
			System.out.println("successor:" + successor);
		}
		InetAddress s = getByName(successor);

		synchronized (successorLock) {
			try {
				this.successor.add(new Successor(s, port, udpPort));
			} catch (IOException e) {
				// super error
				System.err.println("ERR: join successor is dead?");
				resetStates();
				e.printStackTrace();
			}
		}

		synchronized (startKeyLock) {
			this.startKey = new BigInteger(startKey);
		}

		sortSuccessor();
		startCheckSuccessor();
	}

	//TODO
	private void startCheckSuccessor() {
		successorChecker.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				long now = 0;
				if (VERBOSE) {
					now = System.currentTimeMillis();
				}
				sortSuccessor();
				if (VERBOSE) {
					System.out.println("suc check took: " + ((now - System.currentTimeMillis()) / 1000.0) + "s."); 
				}
			}
		}, 5000, 10000);
		
		tokenChecker.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				checkToken();
			}
		}, 5000, 10000);
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

	public static class Successor {
		final public int udpPort;
		final public InetAddress ip;
		final public int tcpPort;
		private boolean isAlive = true;
		private boolean canBeRemoved = false;
		
		private Socket clientSocket;
		
		private Object lock = new Object();

		public Successor(InetAddress ip, int tcpPort, int udpPort) throws IOException {
			this.udpPort = udpPort;
			this.ip = ip;
			this.tcpPort = tcpPort;
			
			clientSocket = new Socket(ip, tcpPort);
		}
		
		public Successor(InetAddress addr) {
			tcpPort = DEFAULT_DHT_TCP_PORT;
			udpPort = Config.validPort[0];
			ip = addr;
		}

		public boolean checkAlive() {
			if (!isAlive) return false;
			try {
				send(new String[] { "alive" }, 1);
				return true;
			} catch (IOException e) {
				setDead();
				return false;
			}
		}
		
		public void send(String header, Object darude) throws IOException {
			synchronized (lock) {
				PrintWriter out = new PrintWriter(
						clientSocket.getOutputStream(), true);
				if (VERBOSE) {
					System.out.println("sending: entrySets");
				}
				out.print(header + "\n");
				out.flush();
				if (out.checkError()) {
					throw new IOException("send object socket closed!");
				}
	
				ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
				oos.flush();
				oos.writeObject(darude);
				oos.flush();
			}
		}
		
		private String readLine() throws IOException {
			return DHT.readLine(clientSocket.getInputStream());
		}
		
		public String[] send(String[] msg, int expectedLinesReturned) throws IOException {
			synchronized (lock) {
				String[] reply = new String[expectedLinesReturned];
				PrintWriter out = new PrintWriter(
						clientSocket.getOutputStream(), true);
				for (int j = 0; j < msg.length; j++) {
					if (VERBOSE) {
						System.out.println("sending:" + msg[j]);
					}
					out.println(msg[j]);
				}
				if (out.checkError()) {
					throw new IOException("socket closed!");
				}
				// if we are expecting a reply
				if (expectedLinesReturned > 0) {
	
					String line;
					int j;
	
					// read in the required lines
					for (j = 0; j < expectedLinesReturned; j++) {
						if ((line = readLine()) != null) {
							if (VERBOSE) {
								System.out.println("reply:" + line);
							}
							reply[j] = line;
						} else {
							// we did not get enough replies, probably mean this
							// successor is dead
							throw new IOException(
									"incorrect # of lines returned");
						}
					}
				}
				out.flush();
				return reply;
			}
		}

		public boolean isAlive() {
			return isAlive;
		}

		public void setDead() {
			synchronized (this) {
				if (!isAlive) {
					return;
				}
				this.isAlive = false;
			}

			new Thread() {
				@Override
				public void run() {
					try {
						// when a dead node is detected, how long until the
						// entry
						// can be safely removed
						//TODO
						sleep(30000);
					} catch (InterruptedException e) {
					}
					try {
						clientSocket = new Socket(ip, tcpPort);
						isAlive = true;
					} catch (IOException e) {
						canBeRemoved = true;
					}
				}
			}.start();
		}

		public boolean canBeRemoved() {
			return canBeRemoved;
		}
		
		public void close() {
			try {
				clientSocket.close();
			} catch (IOException e) {
				System.err.println("ERR: unable to close socket!");
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("unchecked")
	private ArrayList<Successor> getCopy() {
		ArrayList<Successor> clone;
		synchronized (successorLock) {
			clone = (ArrayList<Successor>) successor.clone();
		}
		return clone;
	}

	/**
	 * Will forward msg to the next successor that is alive. It will return any
	 * messages that matches the expectedLinesReturned otherwise it will try the
	 * next sucessor that is alive. If a successor is found dead, it will
	 * initiate the protocol to remove the dead successor. If all successors are
	 * dead, the server will shutdown.
	 * 
	 * @param msg
	 * @param expectedLinesReturned
	 * @return
	 */
	private String[] forward(String[] msg, int expectedLinesReturned, boolean fast, BigInteger key) {
		
		String[] reply = null;
		
		//if (initialNode) {
		//	resetStates();
		//	return reply;
		//}
		
		if (fast) {
			int safety = 2;
			
			List<Darude> l = allNodes;
			int finalDest = Collections.binarySearch(l, key);
			// if not found, that means it is the highest number in the list, so 
			// the highest # is stored on the 1st node
			finalDest = finalDest < 0 ? 0 : finalDest;
				
			int thisNode = Collections.binarySearch(l, endKey);
			thisNode = thisNode < 0 ? 0 : thisNode;
			
			int distance;
			if (finalDest > thisNode) {
				distance = finalDest - thisNode;
			} else {
				distance = l.size() - thisNode + finalDest;
			}
			
			//TODO
			if (l.size() > 15 && distance > maxSuccessor + safety) {
				try {
					int contactNode = finalDest - maxSuccessor - safety;
					contactNode = contactNode < 0 ? contactNode + l.size() : contactNode;
					Darude d = l.get(contactNode);
					Successor su = new Successor(d.addr, DEFAULT_DHT_TCP_PORT, Config.validPort[0]);
					
					reply = su.send(msg, expectedLinesReturned);
					su.close();
					return reply;
				} catch (IOException e) {
					//continue as normal, node we wanted to contact is dead...
				}
			}
		}

		ArrayList<Successor> copy = getCopy();

		int i;
		for (i = 0; i < copy.size(); i++) {
			Successor s = copy.get(i);
			if (!s.isAlive()) {
				if (VERBOSE) {
					System.out.println("successor " + i + " is dead.");
				}
				continue;
			}

			if (VERBOSE) {
				System.out.println("sending to ip:" + s.ip.getHostAddress()
						+ "@" + s.tcpPort);
			}

			try {
				reply = s.send(msg, expectedLinesReturned);
				break;
			} catch (IOException e) {
				// if any of the read or write operations fails, control will
				// continue here, we will log the server that had failed
				// and try the next server
				s.setDead();
				if (VERBOSE) {
					System.out.println("node " + i + " just died. msg:"
							+ e.getMessage());
					e.printStackTrace();
				}
			}
		}

		if (i == copy.size()) {
//			resetStates();
			System.err
					.println("DHT.forward(): All successors are dead. Server shutting down...");
			System.err.println("sucessor.size(): " + copy.size() + " i: " + i);
			System.exit(1);
		}

		return reply;
	}
	
	private int tries = 0;

	private void sortSuccessor() {
		if (sort.tryAcquire()) {
			ArrayList<Successor> alive = new ArrayList<Successor>();
			ArrayList<Successor> weededList = new ArrayList<Successor>();

			// remove all dead successor that can be safely removed
			synchronized (successorLock) {
				int aliveCount = 0; // so that we do not have more than N
									// successor due to updates
				for (int i = 0; i < successor.size()
						&& aliveCount < maxSuccessor; i++) {
					Successor s = successor.get(i);
					if (s.isAlive()) {
						aliveCount++;
						alive.add(s);
					}
					if (!s.canBeRemoved()) {
						weededList.add(s);
					}

				}
				if (LESS_VERBOSE) {
					String print = thisNode.getHostAddress() + "num_successor("
							+ successor.size() + "):";
					for (int i = 0; i < successor.size(); i++) {
						Successor s = successor.get(i);
						if (s.isAlive()) {
							print += s.ip.getHostAddress() + "|";
						}
					}
					System.out.println(print);
				}
				successor = weededList;
			}

			if (alive.size() == 0) {
//				resetStates();
//				sort.release();
//				return;
				tries++;
				//TODO what is the long term effect of this?
				if (tries < 3) {
					sort.release();
					return;
				}
				System.err
						.println("All successors are dead. Server shutting down...");
				System.exit(1);
			}

			tries = 0;
			ArrayList<Successor> newSuccessor = new ArrayList<Successor>();

			// get successor, start from the furthest one
			if (alive.size() < maxSuccessor) {
				for (int i = alive.size() - 1; i >= 0; i--) {
					Successor s = alive.get(i);

					String[] ip = null;
					String[] port = null;
					String[] udp = null;

					try {
						String[] reply = s.send(new String[] { "getSuccessor" }, 3);

						String l1 = reply[0];
						String l2 = reply[1];
						String l3 = reply[2];

						if (VERBOSE) {
							System.out.println("returned successor:");
							System.out.println("ip   : " + l1);
							System.out.println("port : " + l2);
							System.out.println("uport: " + l3);
						}

						if (l1.contains("|")) {
							ip = l1.split("\\|");
							port = l2.split("\\|");
							udp = l3.split("\\|");
						} else {
							ip = new String[] { l1 };
							port = new String[] { l2 };
							udp = new String[] { l3 };
						}

					} catch (NullPointerException | IOException e) {
						// node this was talking to probably died
						// remove it if it's dead
						s.setDead();
						alive.remove(i);
						continue;
					}

					// everything died
					if (alive.size() == 0) {
//						resetStates();
//						return;
						System.err
								.println("All successors are dead. Server shutting down...");
						System.exit(1);
					}

					// add new successor to list
					// stopping before adding itself
					int maxAdd = maxSuccessor - alive.size() < ip.length ? maxSuccessor
							- alive.size()
							: ip.length;
					ArrayList<Successor> copy = getCopy();
					for (int j = 0; j < maxAdd; j++) {
						InetAddress newNode = getByName(ip[j]);
						// stopping before adding it self
						if (newNode.equals(thisNode)) {
							break;
						}
						// do not add duplicates
						// happens when ring is small and some node dies
						boolean addFlag = true;
						for (int k = 0; k < copy.size(); k++) {
							// do not add duplicate nodes
							if (newNode.equals(copy.get(k).ip)) {
								addFlag = false;
								break;
							}
						}
						if (addFlag) {
							try {
								Successor su = new Successor(newNode, Integer
										.parseInt(port[j]), Integer
										.parseInt(udp[j]));
								newSuccessor.add(su);
							} catch (IOException e) {
								// io thrown, node is dead
							}
						}
					}
					break;
				}
			}

			ArrayList<Successor> copy;
			// copy the result over to the master list
			synchronized (successorLock) {
				for (int i = 0; i < newSuccessor.size(); i++) {
					successor.add(newSuccessor.get(i));
				}
				copy = getCopy();
			}

			// check if all nodes in new are alive
			// if not call setDead()
			if (copy.size() > 1) {
				for (int i = 1; i < copy.size(); i++) {
					copy.get(i).checkAlive();
				}
			}

			forward(new String[] { "fail", thisNode.getHostAddress() }, 0, false, null);
			sort.release();
		}
	}
}