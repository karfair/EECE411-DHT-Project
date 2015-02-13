package com.group2.eece411;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.bind.DatatypeConverter;

import com.group2.eece411.Config.Code;
import com.group2.eece411.Config.Code.Command;
import com.group2.eece411.Config.Code.Response;

public class KVServer implements RequestListener {

	private UDPServer server = null;
	private KVStore table;
	private int maxSize = Integer.MAX_VALUE;
	private AtomicBoolean serverIsKilled = new AtomicBoolean(false);

	public KVServer(int maxStorage) {
		this.maxSize = maxStorage;
		table = new KVStore();
	}

	public KVServer() {
		table = new KVStore();
	}

	public void start() {
		if (!serverIsKilled.get()) {
			server = new UDPServer(this);
			server.start();
		}
	}

	public int getPort() {
		if (server != null) {
			return server.getPort();
		} else {
			return -1;
		}
	}

	public void stopMe(int x) {
		try {
			Thread.sleep(x);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		stopMe();
	}

	public void stopMe() {
		if (!serverIsKilled.getAndSet(true)) {
			if (server != null) {
				server.stopMe();
			}
		}
	}

	public void join() {
		if (server != null) {
			try {
				server.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	// handles the message as stated in A3
	// messages w/o the uniqueRequestID
	@Override
	public void handleRequest(byte[] uniqueRequestID, byte[] request,
			InetAddress srcAddr, int srcPort) {
		// if request happens to be null
		if (request == null) {
			byte[] response = new byte[1];
			response[0] = Response.UNRECOGNIZED;
			server.reply(uniqueRequestID, response, srcAddr, srcPort);
			return;
		}

		byte command = request[0];
		byte response;
		byte[] value = null;

		switch (command) {
		case Command.GET:
			if (!isValidKey(request)) {
				response = Response.INVALID_KEY;
				break;
			}
			value = table.get(parseKey(request));
			if (value != null) {
				response = Response.SUCCESS;
			} else {
				response = Response.INVALID_KEY;
			}
			break;
		case Command.PUT:
			if (!isValidKey(request)) {
				response = Response.INVALID_KEY;
				break;
			} else if (table.size() >= maxSize) {
				response = Response.OUT_OF_SPACE;
				break;
			}

			// checks if the value length matches the actual value length
			byte[] valueLength = new byte[Integer.BYTES];
			if (request.length < Code.CMD_LENGTH + Code.KEY_LENGTH
					+ Code.VALUE_LENGTH_LENGTH) {
				response = Response.INVALID_VALUE;
				break;
			}
			// copy the val_len_len out
			System.arraycopy(request, Code.CMD_LENGTH + Code.KEY_LENGTH,
					valueLength, 0, Code.VALUE_LENGTH_LENGTH);
			// length of value as specified by val_length
			int intValueLength = ByteBuffer.wrap(valueLength)
					.order(ByteOrder.LITTLE_ENDIAN).getInt();

			// actual length of value
			int actualValueLength = request.length - Code.CMD_LENGTH
					- Code.VALUE_LENGTH_LENGTH - Code.KEY_LENGTH;

			if (intValueLength > actualValueLength
					|| intValueLength <= 0
					|| intValueLength > Config.MAX_APPLICATION_PAYLOAD
							- Code.CMD_LENGTH - Code.VALUE_LENGTH_LENGTH) {
				response = Response.INVALID_VALUE;
			} else {
				byte[] v = new byte[intValueLength + Code.VALUE_LENGTH_LENGTH];
				System.arraycopy(request, Code.CMD_LENGTH + Code.KEY_LENGTH, v,
						0, v.length);
				table.put(parseKey(request), v);
				response = Response.SUCCESS;
			}

			break;
		case Command.REMOVE:
			if (!isValidKey(request)) {
				response = Response.INVALID_KEY;
				break;
			}
			if (table.remove(parseKey(request)) != null) {
				response = Response.SUCCESS;
			} else {
				response = Response.INVALID_KEY;
			}
			break;
		case Command.SHUTDOWN:
			response = Response.SUCCESS;
			new Thread() {
				@Override
				public void run() {
					stopMe();
				}
			}.start();
			break;
		case Command.ARE_YOU_ALIVE:
			response = Response.I_AM_ALIVE;
			break;
		default:
			response = Response.UNRECOGNIZED;
			break;
		}

		// assemble the data
		byte[] data;
		if (value == null) {
			data = new byte[1];
		} else {
			data = new byte[Code.CMD_LENGTH + value.length];
			System.arraycopy(value, 0, data, Code.CMD_LENGTH, value.length);
		}
		data[0] = response;
		server.reply(uniqueRequestID, data, srcAddr, srcPort);
	}

	private boolean isValidKey(byte[] request) {
		if (request.length < Code.CMD_LENGTH + Code.KEY_LENGTH) {
			return false;
		} else {
			return true;
		}
	}

	// get the key from the message (in hex String format)
	private static String parseKey(byte[] request) {
		byte[] key = new byte[Code.KEY_LENGTH];
		System.arraycopy(request, Code.CMD_LENGTH, key, 0, Code.KEY_LENGTH);
		return DatatypeConverter.printHexBinary(key);
	}
}
