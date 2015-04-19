package com.group2.eece411;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import com.group2.eece411.Config.Code;
import com.group2.eece411.Config.Code.Command;

public class UDPServer extends Thread {

	private static final int MAX_THREADS = 6;
	private static final int MAX_QUEUE = 100;

	// hold on to responses for a while so they aren't repeated
	// creates a processed response list
	private ResponseHolder rs = new ResponseHolder();
	// handles each packet as a thread
	public static ExecutorService requestThreads = Executors
			.newFixedThreadPool(MAX_THREADS);
	public static Semaphore numThreads = new Semaphore(MAX_QUEUE);

	private DatagramSocket socket = null;

	private boolean running = true;

	// upper layer processing
	private RequestListener requestListener;

	// system overload threads
	public static Executor overloadThreads = Executors.newFixedThreadPool(5);

	// fake udp
	public FakeUDP fakeudp;

	public UDPServer(RequestListener requestListener) {
		super("UDPServer");

		// listener for new packet
		this.requestListener = requestListener;

		// tries to create a socket and bind it to the port specified in
		// Configs.validPort
		int i;
		for (i = 0; i < Config.validPort.length; i++) {
			try {
				socket = new DatagramSocket(Config.validPort[i]);
				break;
			} catch (SocketException e) {
				// do nothing
			}
			close();
		}

		// System.out.println(i);
		if (i == Config.validPort.length) {
			close();
			System.err
					.println("Error: Socket cannot be created - all specified port are already in use.");
			System.exit(1);
		}

		// create a channel
		fakeudp = new FakeUDP(this);
		fakeudp.start();
	}

	// stops the server
	// closes socket
	// also stops the collector thread
	// also does a join()
	public void stopMe() {
		running = false;
		requestThreads.shutdown();
		// wait for the processing threads to finish
		try {
			requestThreads.awaitTermination(1, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		close();
		try {
			join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public int getPort() {
		if (socket != null) {
			return socket.getLocalPort();
		} else {
			return -1;
		}
	}

	@Override
	public void run() {
		while (running) {
			// creates receive buffer
			byte[] rcvBuf = new byte[Config.MAX_UDP_PAYLOAD];
			DatagramPacket packet = new DatagramPacket(rcvBuf, rcvBuf.length);
			try {
				// waits for data
				socket.receive(packet);
			} catch (IOException e) {
				if (running == false) {
					break;
				}
				System.err
						.println("Err: receiving failed. Waiting for 100mS and trying again...");
				try {
					Thread.sleep(100);
				} catch (InterruptedException e1) {
					// do nothing
				}
				continue;
			}

			if (!processDatagram(packet)) {
				// means the server is shutting down
				continue;
			}
		}
		// stop and join w/ the collection thread
		rs.stopMe();
	}

	// gets the uniqueRequestID from the packet
	private String getUniqueRequestID(byte[] data) {
		byte[] uniqueRequestID = new byte[Config.REQUEST_ID_LENGTH];
		System.arraycopy(data, 0, uniqueRequestID, 0, Config.REQUEST_ID_LENGTH);
		return DatatypeConverter.printHexBinary(uniqueRequestID);
	}

	private void close() {
		if (socket != null) {
			socket.close();
		}
	}

	private class PacketHandler implements Runnable {

		private DatagramPacket packet;
		private boolean overloaded;

		public PacketHandler(DatagramPacket p, boolean overloaded) {
			this.packet = p;
			this.overloaded = overloaded;
		}

		@Override
		public void run() {
			// get the receive data
			byte[] data = packet.getData();
			
			boolean actuallyOverloaded = overloaded;
			if (overloaded) {
				if (data.length > 16) {
					if (data[16] == Command.RETURN_RESPONSE) {
						overloaded = false;
					}
				}
			}

			if (!overloaded) {
				// get the uniqueRequestID
				byte[] id = new byte[Config.REQUEST_ID_LENGTH];
				System.arraycopy(data, 0, id, 0, Config.REQUEST_ID_LENGTH);

				// checks if this request has already been processed
				DatagramPacket response = rs.get(getUniqueRequestID(id));
				if (response == null) { // if not process, send it to be
										// processed
					byte[] upperLayerData = new byte[packet.getLength()
							- Config.REQUEST_ID_LENGTH];
					System.arraycopy(data, Config.REQUEST_ID_LENGTH,
							upperLayerData, 0, upperLayerData.length);
					requestListener.handleRequest(id, upperLayerData,
							packet.getAddress(), packet.getPort(), false, null,
							0);
				} else {
					// resend the response
					try {
						socket.send(response);
					} catch (IOException e) {
						// failed to send
						System.err
								.println("failed to send processed response.");
						e.printStackTrace();
					}
				}
				if (!actuallyOverloaded) {
					numThreads.release();
				}
			} else {
				// system overload
				byte[] sendBuf = new byte[Config.REQUEST_ID_LENGTH
						+ Code.CMD_LENGTH];
				// copy over unique id
				System.arraycopy(data, 0, sendBuf, 0, Config.REQUEST_ID_LENGTH);
				// put in error code
				sendBuf[Config.REQUEST_ID_LENGTH] = Config.Code.Response.SYSTEM_OVERLOAD;

				// construct datagram
				DatagramPacket d;
				if (data.length > 16) {
					if (data[16] == Command.PASS_REQUEST) {
						// source client
						byte[] ip = new byte[4];
						byte[] port = new byte[4];

						System.arraycopy(data, 17, ip, 0, 4);
						System.arraycopy(data, 17 + 4, port, 0, 4);

						InetAddress src;
						try {
							src = InetAddress.getByAddress(ip);
						} catch (UnknownHostException e) {
							System.err
									.println("system overload:ip address corrupted, dropping message.");
							return;
						}
						int intPort = ByteBuffer.wrap(port).getInt();

						d = new DatagramPacket(sendBuf,
								Config.REQUEST_ID_LENGTH + Code.CMD_LENGTH,
								src, intPort);
					} else {
						d = new DatagramPacket(sendBuf,
								Config.REQUEST_ID_LENGTH + Code.CMD_LENGTH,
								packet.getAddress(), packet.getPort());
					}
				} else {
					System.err
							.println("system overload:message corrupted, dropping message.");
					return;
				}

				try {
					socket.send(d);
				} catch (IOException e1) {
					System.err.println("ERR (system overload): unable to reply to client!");
					e1.printStackTrace();
				}
			}
		}
	}

	// holds processed request for a certain amount of time
	private class ResponseHolder {
		/**
		 * The minimum length of time (in mS) that the server hold on to a
		 * processed packet (a response) with a uniqueRequestID, so that it does
		 * not get reprocessed.
		 */
		//TODO
		private final static int RESPONSE_HOLD_TIME = 20000;

		private ConcurrentHashMap<String, DatagramPacket> map;
		private LinkedBlockingQueue<Response> newResponse;
		private CollectorThread collector;

		public ResponseHolder() {
			map = new ConcurrentHashMap<String, DatagramPacket>();
			newResponse = new LinkedBlockingQueue<Response>();

			// this thread destroys old response record
			collector = new CollectorThread();
			collector.start();
		}

		public DatagramPacket get(String key) {
			return map.get(key);
		}

		public void put(String key, DatagramPacket value) {
			map.put(key, value);
			newResponse.add(new Response(key));
		}

		public void stopMe() {
			collector.stopMe();
		}

		private class CollectorThread extends Thread {

			private boolean running = true;

			public CollectorThread() {
				super("UDPServer.CollectorThread");
			}

			@Override
			public void run() {
				Response r = null;
				while (running) {
					try {
						// attempt to take a new response from a blocking
						// queue
						r = newResponse.take();
					} catch (InterruptedException e) {
						if (!running) {
							break;
						}
						System.err
								.println("Error: Collector thread interrupted while waiting...");
						e.printStackTrace();
					}
					if (r != null) {
						long currentTime = System.currentTimeMillis();
						long creationTime = r.getCreationTime();
						long timeRemaining = RESPONSE_HOLD_TIME
								- (currentTime - creationTime);

						// when the packet has been alive for more than
						// RESPONSE_HOLD_TIME, destroy it otherwise wait
						// until more than RESPONSE_HOLD_TIME has passed,
						// then...
						if (timeRemaining > 0) {
							try {
								Thread.sleep(timeRemaining);
							} catch (InterruptedException e) {
								if (!running) {
									break;
								}
								System.err
										.println("Error: Collector thread interrupted while waiting...");
								e.printStackTrace();
							}
						}
						// delete the old response
						map.remove(r.getUniqueRequestID());
					}
				}
			}

			// stops and waits for the thread to finish
			public void stopMe() {
				running = false;
				interrupt();
				try {
					join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		private class Response {
			final private long creationTime;
			final private String uniqueRequestID;

			public Response(String uniqueRequestID) {
				creationTime = System.currentTimeMillis();
				this.uniqueRequestID = uniqueRequestID;
			}

			public long getCreationTime() {
				return creationTime;
			}

			public String getUniqueRequestID() {
				return uniqueRequestID;
			}
		}
	}

	public boolean reply(byte[] uniqueRequestID, byte[] upperLayerData,
			InetAddress srcAddr, int srcPort) {
		//System.out.println("addr: " + srcAddr.getHostAddress() + "port: "
		//		+ srcPort);

		byte[] sendBuf = new byte[Config.REQUEST_ID_LENGTH
				+ upperLayerData.length];
		// copy over unique id
		System.arraycopy(uniqueRequestID, 0, sendBuf, 0,
				Config.REQUEST_ID_LENGTH);
		// copy the processed data
		System.arraycopy(upperLayerData, 0, sendBuf, Config.REQUEST_ID_LENGTH,
				upperLayerData.length);
		// prepare a packet
		DatagramPacket response = new DatagramPacket(sendBuf, sendBuf.length,
				srcAddr, srcPort);

		// put it in the processed packet list
		rs.put(getUniqueRequestID(uniqueRequestID), response);

		// send/resend the response
		try {
			socket.send(response);
		} catch (IOException e) {
			// failed to send
			// e.printStackTrace();
			return false;
		}
		return true;
	}

	// should append 0x30 , 4 byte of src address, 4 byte of the port, then the
	// original message minus the unique id
	public boolean forwardRequest(byte[] uniqueRequestID, byte[] request,
			InetAddress successor, int successorUDPport, InetAddress srcAddr,
			int srcPort, boolean finalForward, InetAddress srcServer, int sPort) {
		byte[] sendBuf = new byte[Config.REQUEST_ID_LENGTH + Code.CMD_LENGTH
				+ 16 + request.length];
		
		//System.out.println("fwd");
		
		// copy over unique id
		System.arraycopy(uniqueRequestID, 0, sendBuf, 0,
				Config.REQUEST_ID_LENGTH);

		if (finalForward) {
			sendBuf[Config.REQUEST_ID_LENGTH] = Command.RETURN_RESPONSE;
		} else {
			// copy over Command.PASS_REQUEST
			sendBuf[Config.REQUEST_ID_LENGTH] = Command.PASS_REQUEST;
		}

		// copy over srcIP
		byte[] sourceAddress = srcAddr.getAddress();
		System.arraycopy(sourceAddress, 0, sendBuf, Config.REQUEST_ID_LENGTH
				+ Code.CMD_LENGTH, 4);

		// copy over srcport
		byte[] sourcePort = ByteBuffer.allocate(Integer.BYTES).putInt(srcPort)
				.array();
		System.arraycopy(sourcePort, 0, sendBuf, Config.REQUEST_ID_LENGTH
				+ Code.CMD_LENGTH + 4, 4);

		// copy in the passed in server IP
		byte[] serverAddress = srcServer.getAddress();
		System.arraycopy(serverAddress, 0, sendBuf, Config.REQUEST_ID_LENGTH
				+ Code.CMD_LENGTH + 8, 4);

		// copy over this server Port
		byte[] serverPort = ByteBuffer.allocate(Integer.BYTES).putInt(sPort)
				.array();
		System.arraycopy(serverPort, 0, sendBuf, Config.REQUEST_ID_LENGTH
				+ Code.CMD_LENGTH + 12, 4);

		// copy over request
		System.arraycopy(request, 0, sendBuf, Config.REQUEST_ID_LENGTH
				+ Code.CMD_LENGTH + 16, request.length);

		// prepare a packet
		DatagramPacket response;

		// cache the response if get/remove/put happens at this node
		if (finalForward) {
			response = new DatagramPacket(sendBuf, sendBuf.length, srcServer,
					sPort);
			// put it in the processed packet list
			rs.put(getUniqueRequestID(uniqueRequestID), response);
		} else {
			// prepare a packet
			response = new DatagramPacket(sendBuf, sendBuf.length, successor,
					successorUDPport);
		}

		// send/resend the response
		try {
			if (request.length > 1000) {
				FakeUDP.send(response);
			} else {
				socket.send(response);
			}
		} catch (IOException e) {
			// failed to send
			// e.printStackTrace();
			return false;
		}
		return true;
	}

	public boolean processDatagram(DatagramPacket packet) {
		if (numThreads.tryAcquire()) {
			// if resources permits, handle the request
			try {
				requestThreads.execute(new PacketHandler(packet, false));
			} catch (RejectedExecutionException e) {
				// means the server is shutting down
				return false;
			}
		} else {
			// otherwise return overload
			try {
				overloadThreads.execute(new PacketHandler(packet, true));
			} catch (RejectedExecutionException e) {
				// means the server is shutting down
				return false;
			}
		}
		return true;
	}
}
