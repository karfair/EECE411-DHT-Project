package com.group2.eece411;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.ServerSocket;
import java.net.Socket;

public class FakeUDP extends Thread {
	private ServerSocket serverSocket;
	private static int DEFAULT_TCP_PORT = 27411;
	private UDPServer udp;

	private boolean running = true;

	//private Object lock;
	//private ArrayList<SuccessorSender> successor = new ArrayList<SuccessorSender>();

	public FakeUDP(UDPServer udp) {
		super("FakeUDP");
		try {
			serverSocket = new ServerSocket(DEFAULT_TCP_PORT);
		} catch (IOException e) {
			System.err.println("ERROR: FakeUDP, unable to create socket!");
			e.printStackTrace();
			System.exit(1);
		}

		this.udp = udp;
	}

	@Override
	public void run() {
		while (running) {
			try {
				Socket clientSocket = serverSocket.accept();
				UDPServer.overloadThreads.execute(new ClientHandler(clientSocket));
			} catch (IOException e) {
				// probably means it is communicating to has died
				// or the dht is shutting down

				System.err.println("DHT server failed! -> IOException");
				e.printStackTrace();

			}
		}
	}
	
	/*
	@SuppressWarnings("unchecked")
	private ArrayList<SuccessorSender> getCopy() {
		ArrayList<SuccessorSender> clone;
		synchronized (lock) {
			clone = (ArrayList<SuccessorSender>) successor.clone();
		}
		return clone;
	}
	*/

	// http://stackoverflow.com/questions/2878867/how-to-send-an-array-of-bytes-over-a-tcp-connection-java-programming
	public static void send(DatagramPacket p) throws IOException {
		/*
		byte[] data = p.getData();

		if (data[0] == 0x50 || data[0] == 0x51) {
			ArrayList<SuccessorSender> copy = getCopy();
			for (SuccessorSender s : copy) {
				if (s.equals(p.getAddress())) {
					try {
						s.send(p);
						return;
					} catch (IOException e) {
						synchronized (lock) {
							successor.remove(p.getAddress());
						}
						throw e;
					}
				}
			}
			
			synchronized (lock) {
				for (SuccessorSender s : successor) {
					if (s.equals(p.getAddress())) {
						try {
							s.send(p);
							return;
						} catch (IOException e) {
							synchronized (lock) {
								successor.remove(p.getAddress());
							}
							throw e;
						}
					}
				}
				
				SuccessorSender s = new SuccessorSender(p.getAddress());
				successor.add(s);
				
				try {
					s.send(p);
					return;
				} catch (IOException e) {
					synchronized (lock) {
						successor.remove(p.getAddress());
					}
					throw e;
				}
			}
		} else {*/
			Socket client = new Socket(p.getAddress(), DEFAULT_TCP_PORT);
			DataOutputStream dos = new DataOutputStream(client.getOutputStream());
	
			dos.writeInt(p.getLength());
			dos.write(p.getData(), 0, p.getLength());
	
			client.close();
		//}
	}

	/*
	private class SuccessorSender {
		private Socket client;
		private DataOutputStream dos;
		private InetAddress address;

		public SuccessorSender(InetAddress address) throws IOException {
			client = new Socket(address, DEFAULT_TCP_PORT);
			dos = new DataOutputStream(client.getOutputStream());
			this.address = address;
		}

		public synchronized void send(DatagramPacket p) throws IOException {
			dos.writeInt(p.getLength());
			dos.write(p.getData(), 0, p.getLength());
		}

		public InetAddress getAddress() {
			return address;
		}

		@Override
		public boolean equals(Object obj) {
			return address.equals(obj);
		}
	}*/

	private class ClientHandler implements Runnable {

		Socket clientSocket;

		public ClientHandler(Socket clientSocket) {
			//super("ClientHandler");
			this.clientSocket = clientSocket;
		}

		@Override
		public void run() {
			try {
				DataInputStream dis = new DataInputStream(
						clientSocket.getInputStream());
				int len;

				len = dis.readInt();
				byte[] data = new byte[len];
				dis.readFully(data);
				clientSocket.close();

				udp.processDatagram(new DatagramPacket(data, data.length, clientSocket.getInetAddress(), Config.validPort[0]));

			} catch (Exception e) {
				// socket has been closed, do nothing
				System.err.println("ERROR rcving msg!");
				e.printStackTrace();
			}
		}
	}
}
