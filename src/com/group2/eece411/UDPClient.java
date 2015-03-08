package com.group2.eece411;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Random;

import javax.xml.bind.DatatypeConverter;

public class UDPClient implements Closeable {

	private final static int NUM_TRIES = 20;
	private final static int TIMEOUT_MULTIPLIER = 1;
	private final static int TIMEOUT_VALUE_MS = 400;

	private int destPort;
	private InetAddress destAddress = null;
	private DatagramSocket socket = null;

	private int defaultTimeout;
	private int defaultMultiplier;
	private int defaultTries;

	private byte[] rcvBuf;
	private byte[] sendBuf;

	// part of the uniqueRequestID
	byte[] nHostIP = null;
	byte[] nPort;

	public UDPClient(String destName, int destPort) {
		this(destName, destPort, Config.MAX_UDP_PAYLOAD,
				Config.MAX_UDP_PAYLOAD, TIMEOUT_VALUE_MS, TIMEOUT_MULTIPLIER,
				NUM_TRIES);
	}

	public UDPClient(String destName, int destPort, int defaultTimeout,
			int defaultMultiplier, int defaultTries) {
		this(destName, destPort, Config.MAX_UDP_PAYLOAD,
				Config.MAX_UDP_PAYLOAD, defaultTimeout, defaultMultiplier,
				defaultTries);
	}

	public UDPClient(String destName, int destPort, int sendBufferSize,
			int rcvBufferSize, int defaultTimeout, int defaultMultiplier,
			int defaultTries) {

		rcvBuf = new byte[rcvBufferSize];
		sendBuf = new byte[sendBufferSize];

		this.defaultTimeout = defaultTimeout;
		this.defaultMultiplier = defaultMultiplier;
		this.defaultTries = defaultTries;

		changeDest(destName, destPort);
	}

	public void changeDest(String destName, int destPort) {
		// closes the previous socket if it exists
		close();

		// changes the port
		this.destPort = destPort;

		// sets up new socket and the destAddress
		try {
			socket = new DatagramSocket();
			destAddress = InetAddress.getByName(destName);
		} catch (IOException e) {
			close();
			System.err.println("Error: Socket cannot be created!");
			e.printStackTrace();
			System.exit(1);
		}
		// System.out.println("local udp port:" + socket.getLocalPort());

		// sets up part of the unique requestID

		// get IP address in big endian format
		// 4 bytes
		nHostIP = null;
		try {
			nHostIP = InetAddress.getLocalHost().getAddress();
		} catch (UnknownHostException e) {
			close();
			System.err
					.println("Error: Unable to resolve the IP address of this host!");
			e.printStackTrace();
			System.exit(1);
		}

		// get port number in big endian format
		// 2 bytes
		nPort = ByteBuffer.allocate(Integer.BYTES)
				.putInt(socket.getLocalPort()).array();
	}

	@Override
	// forces the user to close the socket after they're done
	public void close() {
		if (socket != null) {
			socket.close();
		}
	}

	public void setTimeOut(int timeout) {
		defaultTimeout = timeout;
	}

	public void setMultiplier(int multiplier) {
		defaultMultiplier = multiplier;
	}

	public void setNumTries(int numTries) {
		defaultTries = numTries;
	}

	public byte[] sendAndWaitFor(byte[] data, int time, int multiplier,
			int tries) throws IOException {
		byte[] rcvMsg = null;
		try {
			// put the "unique request ID" in the sendBuffer in big endian
			// format
			byte[] uniqueRequestID = getUniqueRequestID();
			System.arraycopy(uniqueRequestID, 0, sendBuf, 0,
					uniqueRequestID.length);

			// put the data in the sendBuffer
			System.arraycopy(data, 0, sendBuf, uniqueRequestID.length,
					data.length);

			// send packet init
			DatagramPacket packet = new DatagramPacket(sendBuf,
					uniqueRequestID.length + data.length, destAddress, destPort);

			// receive packet init
			DatagramPacket rcvpacket = new DatagramPacket(rcvBuf, rcvBuf.length);

			// resets timeout
			int timeout = time;
			socket.setSoTimeout(timeout);

			// receive buffer
			byte[] rcv = null;

			// sends and waits for reply (NUM_TRIES times, doubling the timeout
			// on each packet lost)
			int i;
			for (i = 0; i < tries; i++) {
				socket.send(packet);
				try {
					for (int j = 0; j < tries; j++) {
						// receive and extract data
						socket.receive(rcvpacket);
						rcv = rcvpacket.getData();

						// checks if uniqueReplyID == uniqueSendID
						if (idEquals(uniqueRequestID, rcv)) {
							break;
						} else {
							// wrong id
						}
					}
					if (idEquals(uniqueRequestID, rcv)) {
						break;
					}
				} catch (IOException e) {
					// timedOut
					// intentionally empty
				}

				// times the timeout by the multiplier
				timeout *= multiplier;
				socket.setSoTimeout(timeout);
			}

			if (i == tries) {
				String msg = "Send failed: incorrect/no reponse from server after "
						+ tries
						+ " tries. uniqueID: "
						+ DatatypeConverter.printHexBinary(uniqueRequestID);
				throw new IOException(msg);
			}

			// gets the data minus the unique reply id
			// TODO this should just be the msg, not the whole buffer, see below
			rcvMsg = new byte[rcvpacket.getLength() - Config.REQUEST_ID_LENGTH];
			System.arraycopy(rcv, Config.REQUEST_ID_LENGTH, rcvMsg, 0,
					rcvpacket.getLength() - Config.REQUEST_ID_LENGTH);

		} catch (IOException e) {
			throw e;
		}

		// sends back data to the upper layer
		return rcvMsg;
	}

	public byte[] sendAndWait(byte[] data) throws IOException {
		byte[] rcvMsg = null;
		try {
			// put the "unique request ID" in the sendBuffer in big endian
			// format
			byte[] uniqueRequestID = getUniqueRequestID();
			System.arraycopy(uniqueRequestID, 0, sendBuf, 0,
					uniqueRequestID.length);

			// put the data in the sendBuffer
			System.arraycopy(data, 0, sendBuf, uniqueRequestID.length,
					data.length);

			// send packet init
			DatagramPacket packet = new DatagramPacket(sendBuf,
					uniqueRequestID.length + data.length, destAddress, destPort);

			// receive packet init
			DatagramPacket rcvpacket = new DatagramPacket(rcvBuf, rcvBuf.length);

			// resets timeout
			int timeout = defaultTimeout;
			socket.setSoTimeout(timeout);

			// receive buffer
			byte[] rcv = null;

			// sends and waits for reply (NUM_TRIES times, doubling the timeout
			// on each packet lost)
			int i;
			for (i = 0; i < defaultTries; i++) {
				socket.send(packet);
				try {
					for (int j = 0; j < defaultTries; j++) {
						// receive and extract data
						socket.receive(rcvpacket);
						rcv = rcvpacket.getData();

						// checks if uniqueReplyID == uniqueSendID
						if (idEquals(uniqueRequestID, rcv)) {
							break;
						} else {
							// wrong id
						}
					}
					if (idEquals(uniqueRequestID, rcv)) {
						break;
					}
				} catch (IOException e) {
					// timedOut
					// intentionally empty
				}

				// times the timeout by the multiplier
				timeout *= defaultMultiplier;
				socket.setSoTimeout(timeout);
			}

			if (i == defaultTries) {
				String msg = "Send failed: incorrect/no reponse from server after "
						+ defaultTries
						+ " tries. uniqueID: "
						+ DatatypeConverter.printHexBinary(uniqueRequestID);
				throw new IOException(msg);
			}

			// gets the data minus the unique reply id
			rcvMsg = new byte[rcv.length - Config.REQUEST_ID_LENGTH];
			System.arraycopy(rcv, Config.REQUEST_ID_LENGTH, rcvMsg, 0,
					rcv.length - Config.REQUEST_ID_LENGTH);

		} catch (IOException e) {
			throw e;
		}

		// sends back data to the upper layer
		return rcvMsg;
	}

	// checks if the replyId is the same as the requestId
	private boolean idEquals(byte[] requestId, byte[] replyId) {
		for (int i = 0; i < Config.REQUEST_ID_LENGTH; i++) {
			if (requestId[i] != replyId[i]) {
				return false;
			}
		}
		return true;
	}

	// sets up "unique request ID"
	private byte[] getUniqueRequestID() {
		byte[] uniqueRequestID = new byte[Config.REQUEST_ID_LENGTH];

		// put in this port# and this hostIP
		System.arraycopy(nHostIP, 0, uniqueRequestID, 0, nHostIP.length);
		System.arraycopy(nPort, Integer.BYTES - 2, uniqueRequestID, 4, 2);

		// get random bytes
		// 2 bytes
		byte[] nRandom = new byte[2];
		new Random().nextBytes(nRandom);
		System.arraycopy(nRandom, 0, uniqueRequestID, 6, nRandom.length);

		// get time in big endian format
		// 8 bytes
		byte[] nTime = ByteBuffer.allocate(8)
				.putLong(System.currentTimeMillis()).array();
		System.arraycopy(nTime, 0, uniqueRequestID, 8, nTime.length);

		return uniqueRequestID;
	}
}
