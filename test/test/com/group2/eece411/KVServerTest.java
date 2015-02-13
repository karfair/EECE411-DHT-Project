package test.com.group2.eece411;

import org.junit.Before;

import com.group2.eece411.KVServer;

public class KVServerTest {

	private KVServer server;

	@Before
	public void setUp() {
		server = new KVServer();
	}

	// @Test
	// public void putAndGet() {
	// byte[] request = new byte[36];
	// request[0] = Command.PUT;
	// request[12] = 5; // key value
	// request[33] = 1; // val_len = 1
	// request[35] = 111; // value
	// byte[] response = server.handleRequest(request);
	// assertEquals(Response.SUCCESS, response[0]);
	//
	// request[0] = Command.GET;
	// response = server.handleRequest(request);
	// assertEquals(Response.SUCCESS, response[0]);
	// assertEquals(request[35], response[3]);
	// }
	//
	// @Test
	// public void putAndGetAndRemove() {
	// putAndGet();
	//
	// byte[] request = new byte[33];
	// request[0] = Command.REMOVE;
	// request[12] = 5;
	// server.handleRequest(request);
	//
	// request[0] = Command.GET;
	// byte[] response = server.handleRequest(request);
	// assertEquals(Response.INVALID_KEY, response[0]);
	// }
	//
	// @Test
	// public void invalidRemoveTest() {
	// byte[] request = new byte[33];
	// request[0] = Command.REMOVE;
	// request[12] = 5;
	// byte[] response = server.handleRequest(request);
	// assertEquals(Response.INVALID_KEY, response[0]);
	// }
	//
	// @Test
	// public void areYouAliveTest() {
	// byte[] request = new byte[1];
	// request[0] = Command.ARE_YOU_ALIVE;
	// byte[] response = server.handleRequest(request);
	// assertEquals(Response.I_AM_ALIVE, response[0]);
	// }
	//
	// @Test
	// public void unrecognizedTest() {
	// byte[] request = new byte[1];
	// request[0] = -33;
	// byte[] response = server.handleRequest(request);
	// assertEquals(Response.UNRECOGNIZED, response[0]);
	// }
	//
	// @Test
	// public void invalidValueTest() {
	// // empty val but w/ a val length
	// byte[] request = new byte[35];
	// request[0] = Command.PUT;
	// request[12] = 5;
	// request[33] = 1;
	// byte[] response = server.handleRequest(request);
	// assertEquals(Response.INVALID_VALUE, response[0]);
	//
	// // empty val and val length
	// request = new byte[33];
	// request[0] = Command.PUT;
	// request[12] = 5;
	// response = server.handleRequest(request);
	// assertEquals(Response.INVALID_VALUE, response[0]);
	//
	// // negative value length
	// request = new byte[35];
	// request[0] = Command.PUT;
	// request[12] = 5;
	// request[33] = -12;
	// response = server.handleRequest(request);
	// assertEquals(Response.INVALID_VALUE, response[0]);
	//
	// // too large a value
	// request = new byte[35 + 65535];
	// request[0] = Command.PUT;
	// request[12] = 5;
	// request[33] = (byte) 0xFF; // this is 65535
	// request[34] = (byte) 0xFF;
	// response = server.handleRequest(request);
	// assertEquals(Response.INVALID_VALUE, response[0]);
	//
	// // 0 val length, no value
	// request = new byte[35];
	// request[0] = Command.PUT;
	// request[12] = 5;
	// response = server.handleRequest(request);
	// assertEquals(Response.INVALID_VALUE, response[0]);
	// }
	//
	// @Test
	// public void shutDownTest() {
	// byte[] request = new byte[1];
	// request[0] = Command.SHUTDOWN;
	// byte[] response = server.handleRequest(request);
	// assertEquals(Response.SUCCESS, response[0]);
	// }
	//
	// @Test
	// public void nullMsgTest() {
	// // msg is null
	// byte[] response = server.handleRequest(null);
	// assertEquals(Response.UNRECOGNIZED, response[0]);
	// }
	//
	// @Test
	// public void badKeyTest() {
	// byte[] request = new byte[2];
	// request[1] = 1;
	//
	// // put
	// request[0] = Command.PUT;
	// byte[] response = server.handleRequest(request);
	// assertEquals(Response.INVALID_KEY, response[0]);
	//
	// // get
	// request[0] = Command.GET;
	// response = server.handleRequest(request);
	// assertEquals(Response.INVALID_KEY, response[0]);
	//
	// // remove
	// request[0] = Command.REMOVE;
	// response = server.handleRequest(request);
	// assertEquals(Response.INVALID_KEY, response[0]);
	// }
}
