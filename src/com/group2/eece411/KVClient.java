package com.group2.eece411;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.group2.eece411.Config.Code;
import com.group2.eece411.Config.Code.Command;
import com.group2.eece411.Config.Code.Response;

public class KVClient implements Closeable {

	private UDPClient u;

	public KVClient(String destHost) {
		// tries to connect to one of the port specified in Configs.validPort
		int i;
		for (i = 0; i < Config.validPort.length; i++) {
			u = new UDPClient(destHost, Config.validPort[i]);
			if (isAlive()) {
				break;
			}
			u.close();
		}
		// cannot connect
		if (i == Config.validPort.length) {
			System.err.println("Error: cannot connect to server.");
			System.exit(1);
		}
	}

	public KVClient(String destHost, int destPort) {
		u = new UDPClient(destHost, destPort);
	}

	public void setTimeOut(int timeout) {
		u.setTimeOut(timeout);
	}

	public void setMultiplier(int multiplier) {
		u.setMultiplier(multiplier);
	}

	public void setNumTries(int numTries) {
		u.setNumTries(numTries);
	}

	public boolean put(byte[] key, byte[] value) {
		// check if inputs are valid
		if (!isValidKey(key)) {
			System.err.println("put(): invalid key");
			return false;
		} else if (value == null) {
			System.err.println("put(): invalid value");
			return false;
		}
		int requestLength = Code.CMD_LENGTH + Code.KEY_LENGTH
				+ Code.VALUE_LENGTH_LENGTH + value.length;
		if (requestLength > Config.MAX_APPLICATION_PAYLOAD) {
			return false;
		}

		byte[] request = new byte[requestLength];
		request[0] = Code.Command.PUT;

		// put in the key
		System.arraycopy(key, 0, request, Code.CMD_LENGTH, Code.KEY_LENGTH);

		// put in the value length (little endian)
		byte[] valLength = ByteBuffer.allocate(4)
				.order(ByteOrder.LITTLE_ENDIAN).putInt(value.length).array();
		System.arraycopy(valLength, 0, request, Code.CMD_LENGTH
				+ Code.KEY_LENGTH, Code.VALUE_LENGTH_LENGTH);

		// put in the value
		System.arraycopy(value, 0, request, Code.CMD_LENGTH + Code.KEY_LENGTH
				+ Code.VALUE_LENGTH_LENGTH, value.length);

		byte[] rcv = null;
		try {
			rcv = u.sendAndWait(request);
		} catch (IOException e) {
			// e.printStackTrace();
			System.err.println("put():no reply:" + e.getMessage());
			return false;
		}

		// check if response is correct and check if put operation is successful
		if (rcv[0] == Response.SUCCESS) {
			return true;
		} else {
			System.err.println("put():error code:" + rcv[0]);
			return false;
		}
	}

	public byte[] get(byte[] key) {
		if (!isValidKey(key)) {
			System.err.println("get(): invalid key");
			return null;
		}

		byte[] request = new byte[Code.CMD_LENGTH + Code.KEY_LENGTH];
		request[0] = Code.Command.GET;

		System.arraycopy(key, 0, request, Code.CMD_LENGTH, Code.KEY_LENGTH);

		byte[] rcv = null;
		try {
			rcv = u.sendAndWait(request);
		} catch (IOException e) {
			System.err.println("get():no reply:" + e.getMessage());
			return null;
		}

		if (rcv[0] == Response.SUCCESS) {
			// checks if the value length matches the actual value length
			byte[] valueLength = new byte[Integer.BYTES];

			// copy the val_len_len out
			System.arraycopy(rcv, Code.CMD_LENGTH, valueLength, 0,
					Code.VALUE_LENGTH_LENGTH);

			// length of value as specified by val_length
			int intValueLength = ByteBuffer.wrap(valueLength)
					.order(ByteOrder.LITTLE_ENDIAN).getInt();

			// copy the delivered data out
			byte[] data = new byte[intValueLength];
			System.arraycopy(rcv, Code.CMD_LENGTH + Code.VALUE_LENGTH_LENGTH,
					data, 0, intValueLength);
			return data;
		} else {
			System.err.println("get(): error code:" + rcv[0]);
			return null;
		}
	}

	public boolean remove(byte[] key) {
		if (!isValidKey(key)) {
			System.err.println("get(): invalid key");
			return false;
		}

		byte[] request = new byte[Code.CMD_LENGTH + Code.KEY_LENGTH];
		request[0] = Code.Command.REMOVE;

		System.arraycopy(key, 0, request, Code.CMD_LENGTH, Code.KEY_LENGTH);

		byte[] rcv = null;
		try {
			rcv = u.sendAndWait(request);
		} catch (IOException e) {
			System.err.println("remove():no reply:" + e.getMessage());
			return false;
		}

		// check if response is correct and check if remove operation is
		// successful
		if (rcv[0] == Response.SUCCESS) {
			return true;
		} else {
			System.err.println("remove(): error code:" + rcv[0]);
			return false;
		}
	}

	public boolean killAndWait() {
		byte[] request = new byte[Code.CMD_LENGTH];
		request[0] = Command.SHUTDOWN;
		byte[] rcv = null;
		try {
			rcv = u.sendAndWait(request);
		} catch (IOException e) {
			return false;
		}
		if (rcv == null) {
			return false;
		} else if (rcv[0] == Response.SUCCESS) {
			return true;
		}
		return false;
	}

	public void kill() {
		byte[] request = new byte[Code.CMD_LENGTH];
		request[0] = Command.SHUTDOWN;
		try {
			u.sendAndWaitFor(request, 50, 1, 1);
		} catch (IOException e) {
		}
		return;
	}

	public byte[] getAllNodes() {
		byte[] request = new byte[Code.CMD_LENGTH];
		request[0] = Command.GET_ALL_NODES;
		byte[] rcv = null;
		try {
			rcv = u.sendAndWaitFor(request, 10000, 1, 5); // 10 sec
															// 1 multiplier
															// 5tries
		} catch (IOException e) {
			System.err.println("getAllNodes(): no reply: " + e.getMessage());
			return null;
		}

		if (rcv[0] == Response.SUCCESS) {
			byte[] ret = new byte[rcv.length - 1];
			System.arraycopy(rcv, 1, ret, 0, rcv.length - 1);
			return ret;
		} else {
			System.out.println("getAllNodes(): error code:" + rcv[0]);
			return null;
		}
	}

	// check is a node is alive at the IP&port inputed
	public boolean isAlive() {
		byte[] request = new byte[Code.CMD_LENGTH];
		request[0] = Command.ARE_YOU_ALIVE;
		byte[] rcv = null;
		try {
			rcv = u.sendAndWait(request);
		} catch (IOException e) {
			return false;
		}
		if (rcv == null) {
			return false;
		} else if (rcv[0] == Response.I_AM_ALIVE) {
			return true;
		}
		return false;
	}

	private boolean isValidKey(byte[] key) {
		if (key == null) {
			return false;
		} else if (key.length != Code.KEY_LENGTH) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public void close() {
		u.close();
	}
}
