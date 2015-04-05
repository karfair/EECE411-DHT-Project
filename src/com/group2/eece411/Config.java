package com.group2.eece411;

/**
 * 
 * @author Phil
 *
 */
public class Config {
	/**
	 * Length of the unique request ID that is used to identify the message
	 */
	public final static int REQUEST_ID_LENGTH = 16;

	/**
	 * The port numbers that the server will attempt to bind the socket to. The
	 * server will attempt to bind with validPort[0] first, if that is not
	 * available, it will try validPort[1], and so on...
	 */
	public final static int[] validPort = { 6772, 52180, 7632, 30541, 27411 };

	public final static int MAX_APPLICATION_PAYLOAD = 16384;
	public final static int MAX_UDP_PAYLOAD = MAX_APPLICATION_PAYLOAD
			+ REQUEST_ID_LENGTH;
    public final static int IP_ADDRESS_LENGTH = 4;

	public static class Code {

		/**
		 * Length of the Commands/Response sent to/from the server (in bytes).
		 */
		public final static byte CMD_LENGTH = 1;
		/**
		 * Length of the KVStore key (in bytes).
		 */
		public final static byte KEY_LENGTH = 32;
		/**
		 * Length of the valueLength in the request/response messages (in
		 * bytes).
		 */
		public final static byte VALUE_LENGTH_LENGTH = 2;

        //Largest is 0x7F
		public static class Command {
			public final static byte PUT = 0x01;
			public final static byte GET = 0x02;
			public final static byte REMOVE = 0x03;
			/**
			 * Command to shutdown the node.
			 */
			public final static byte SHUTDOWN = 0x04;
			/**
			 * Command to see if the Node is alive.
			 */
			public final static byte ARE_YOU_ALIVE = 0x20;

			public final static byte PASS_REQUEST = 0x30;
			public final static byte RETURN_RESPONSE = 0x31;

			public final static byte GET_ALL_NODES = 0x40;

            public final static byte FORCE_PUT = 0x50;
            public final static byte FORCE_REMOVE = 0x51;

            public final static byte FORCE_GET = 0x52;

            public final static byte ONLY_ONE = 0x06;
		}

		public static class Response {
			/**
			 * This means the operation is successful.
			 */
			public final static byte SUCCESS = 0x00;
			/**
			 * Non-existent key requested in a get or delete operation
			 */
			public final static byte INVALID_KEY = 0x01;
			/**
			 * Out of space (returned when there is no space left for a put).
			 */
			public final static byte OUT_OF_SPACE = 0x02;
			/**
			 * System overload.
			 */
			public final static byte SYSTEM_OVERLOAD = 0x03;
			/**
			 * Internal KVStore failure
			 */
			public final static byte INTERNAL_FAILURE = 0x04;
			/**
			 * Unrecognized command.
			 */
			public final static byte UNRECOGNIZED = 0x05;
			/**
			 * Node is alive.
			 */
			public final static byte I_AM_ALIVE = 0x20;
			/**
			 * Value is too long, or wrongly formatted.
			 */
			public final static byte INVALID_VALUE = 0x21;
		}
	}
}
