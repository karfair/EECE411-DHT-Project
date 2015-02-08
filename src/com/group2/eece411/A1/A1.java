package com.group2.eece411.A1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import javax.xml.bind.DatatypeConverter;

import com.group2.eece411.UDPClient;

public class A1 {
	public static void main(String[] args) {
		// default values
		String hostName = "168.235.153.23";
		String port = "5627";
		String sid = "37748118";

		int bufferSize = 16384;
		int timeoutMS = 100;

		// if inputed, use these values instead
		if (args.length == 3) {
			hostName = args[0];
			port = args[1];
			sid = args[2];
			System.out.println("Using inputed values");
		} else if (args.length != 0) {
			System.out
					.println("Invalid number of arguments. Using default values.");
		} else {
			System.out.println("Using default values.");
		}
		System.out.println("IP: " + hostName + " Port: " + port + " SID: "
				+ sid);

		// creates a new UDPClient
		UDPClient u = new UDPClient(hostName, Integer.parseInt(port),
				bufferSize, bufferSize, timeoutMS);

		// turns the SID into a byte array (little endian)
		byte[] nSid = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
				.putInt(Integer.parseInt(sid)).array();

		// sends the SID
		byte[] rcv = null;
		try {
			rcv = u.sendAndWait(nSid);
		} catch (IOException e) {
			u.close();
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

		// gets the secret code length
		byte[] codeLength = new byte[4];
		System.arraycopy(rcv, 0, codeLength, 0, codeLength.length);
		int intCodeLength = ByteBuffer.wrap(codeLength)
				.order(ByteOrder.LITTLE_ENDIAN).getInt();
		System.out.println("Secret Code Length: " + intCodeLength);

		// gets secret code
		byte[] secretCode = new byte[16];
		System.arraycopy(rcv, 4, secretCode, 0, intCodeLength);
		System.out.println("Secret: "
				+ DatatypeConverter.printHexBinary(secretCode));

		u.close();
	}
}
