package com.group2.eece411;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
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

public class UDPServer extends Thread {

	private static final int MAX_THREADS = 4;
	private static final int MAX_QUEUE = 20;

	// hold on to responses for a while so they aren't repeated
	// creates a processed response list
	private ResponseHolder rs = new ResponseHolder();
	// handles each packet as a thread
	private ExecutorService requestThreads = Executors
			.newFixedThreadPool(MAX_THREADS);
	private Semaphore numThreads = new Semaphore(MAX_QUEUE);

	private DatagramSocket socket = null;

	private boolean running = true;

	// upper layer processing
	private RequestListener requestListener;

	// system overload threads
	private Executor overloadThreads = Executors.newFixedThreadPool(2);

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

			if (numThreads.tryAcquire()) {
				// if resources permits, handle the request
				try {
					requestThreads.execute(new PacketHandler(packet, false));
				} catch (RejectedExecutionException e) {
					// means the server is shutting down
					continue;
				}
			} else {
				// otherwise return overload
				try {
					overloadThreads.execute(new PacketHandler(packet, true));
				} catch (RejectedExecutionException e) {
					// means the server is shutting down
					continue;
				}
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
							packet.getAddress(), packet.getPort());
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
				numThreads.release();
			} else {
				byte[] sendBuf = new byte[Config.REQUEST_ID_LENGTH
						+ Code.CMD_LENGTH];
				// copy over unique id
				System.arraycopy(data, 0, sendBuf, 0, Config.REQUEST_ID_LENGTH);
				// put in error code
				sendBuf[Config.REQUEST_ID_LENGTH] = Config.Code.Response.SYSTEM_OVERLOAD;

				// construct datagram
				DatagramPacket d = new DatagramPacket(sendBuf,
						Config.REQUEST_ID_LENGTH + Code.CMD_LENGTH,
						packet.getAddress(), packet.getPort());

				try {
					socket.send(d);
				} catch (IOException e1) {
					// e1.printStackTrace();
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
		private final static int RESPONSE_HOLD_TIME = 1500;

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
}
