package com.group2.eece411;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
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

	private DHT dht;

	public KVServer(int maxStorage, boolean initialNode,
			String initialNodeName, int port) {
		this.maxSize = maxStorage;
		table = new KVStore();

		server = new UDPServer(this);
		this.dht = new DHT(initialNode, initialNodeName, port, server.getPort());
	}

	public KVServer(boolean initialNode, String initialNodeName, int port) {
		table = new KVStore();

		server = new UDPServer(this);
		this.dht = new DHT(initialNode, initialNodeName, port, server.getPort());
	}

	public void start() {
		if (!serverIsKilled.get()) {
			server.start();
			dht.start();
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
		dht.stopMe();
	}

	public void stopMe() {
		if (!serverIsKilled.getAndSet(true)) {
			if (server != null) {
				server.stopMe();
				dht.stopMe();
			}
		}
	}

	public void join() {
		if (server != null) {
			try {
				server.join();
				dht.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	// handles the message as stated in A3
	// messages w/o the uniqueRequestID
	@Override
	public void handleRequest(byte[] uniqueRequestID, byte[] request,
			InetAddress srcAddr, int srcPort, boolean routeMsgTo,
			InetAddress srcServer, int serverPort) {
		// if request happens to be null
		if (request == null) {
			byte[] response = new byte[1];
			response[0] = Response.UNRECOGNIZED;
			server.reply(uniqueRequestID, response, srcAddr, srcPort);
			return;
		}

		System.out.println("request_length: " + request.length);

		byte command = request[0];
		byte response;
		byte[] value = null;

		switch (command) {
		case Command.GET:
			if (!isValidKey(request)) {
				response = Response.INVALID_KEY;
				break;
			} else if (!isKeyInRange(request)) {
				if (!routeMsgTo) {
					srcServer = dht.getLocalHost();
					serverPort = server.getPort();
				}
				DHT.Successor s = closestSuccessorTo(request);
				server.forwardRequest(uniqueRequestID, request, s.ip,
						s.udpPort, srcAddr, srcPort, false, srcServer,
						serverPort);
				return;
			}

			value = table.get(parseKey(request));
			if (value != null) {
				response = Response.SUCCESS;
                break;
			} else {
                /*
                If the key is not in this node,pass the get command to its first two successors.
                Message passed contains: Command(1byte) + key + IpAdress of this node + IpAdress of first Successor + 2nd Successor
                 */
                //find first 2 successors
				// put in this port
				srcServer = dht.getLocalHost();
				serverPort = server.getPort();
				
                ArrayList<DHT.Successor> firstTWo = dht.firstTwoSuccessor();
                if(firstTWo.isEmpty()){
                    response = Response.INVALID_KEY;
                    break;
                }

                byte[] newReq = new byte[Code.CMD_LENGTH + 3*Config.IP_ADDRESS_LENGTH + Code.KEY_LENGTH];
                //put command in request
                newReq[0] = Command.FORCE_GET;
                //adds key to request
                byte[] key = new byte[Code.KEY_LENGTH];
                System.arraycopy(request, Code.CMD_LENGTH, key, 0, Code.KEY_LENGTH);
                System.arraycopy(key,0,newReq,Code.CMD_LENGTH,Code.KEY_LENGTH);
                //add own ip to request
                System.arraycopy(dht.getLocalHost().getAddress(),0,newReq,Code.CMD_LENGTH + Code.KEY_LENGTH,Config.IP_ADDRESS_LENGTH);

                if(firstTWo.size() == 1){
                    System.arraycopy(firstTWo.get(0).ip.getAddress(),0,newReq,Config.IP_ADDRESS_LENGTH + Code.CMD_LENGTH + Code.KEY_LENGTH,Config.IP_ADDRESS_LENGTH);
                }else {
                    //iterate the two successors and add their ip to request
                    int count = 0;
                    for (DHT.Successor s : firstTWo) {
                        System.arraycopy(s.ip.getAddress(), 0, newReq, (count + 1) * Config.IP_ADDRESS_LENGTH + Code.CMD_LENGTH + Code.KEY_LENGTH, Config.IP_ADDRESS_LENGTH);
                        count++;
                    }
                }

                server.forwardRequest(uniqueRequestID, newReq, firstTWo.get(0).ip, Config.validPort[0], srcAddr, srcPort, false, srcServer, serverPort);

                return;
			}
        case Command.FORCE_GET:
        	System.out.println("//////////////\n2nd pass.");
        	
            byte[] firstNode = new byte[dht.getLocalHost().getAddress().length];
            byte[] secondNode = new byte[dht.getLocalHost().getAddress().length];
            byte[] thirdNode = new byte[dht.getLocalHost().getAddress().length];
            byte[] key = new byte[Code.KEY_LENGTH];

            System.arraycopy(request,Code.CMD_LENGTH,key,0,Code.KEY_LENGTH);

            System.arraycopy(request,Code.CMD_LENGTH + Code.KEY_LENGTH,firstNode,0,Config.IP_ADDRESS_LENGTH);

            System.arraycopy(request,Code.CMD_LENGTH+Config.IP_ADDRESS_LENGTH + Code.KEY_LENGTH,secondNode,0,Config.IP_ADDRESS_LENGTH);

            System.arraycopy(request,Code.CMD_LENGTH+2*Config.IP_ADDRESS_LENGTH + Code.KEY_LENGTH,thirdNode,0,Config.IP_ADDRESS_LENGTH);

            value = table.get(parseKey(request));
            System.out.println("first:" + DatatypeConverter.printHexBinary(firstNode));
            System.out.println("2nd  :" + DatatypeConverter.printHexBinary(secondNode));
            System.out.println("3rd  :" + DatatypeConverter.printHexBinary(thirdNode));
        	System.out.println("this :" + DatatypeConverter.printHexBinary(dht.getLocalHost().getAddress()));
            
            if(Arrays.equals(dht.getLocalHost().getAddress(),secondNode)) {
            	System.out.println("2nd pass.");
            	byte[] only_one = new byte[Config.IP_ADDRESS_LENGTH];
                if (value != null) {
                    response = Response.SUCCESS;
                    byte[] newReq = new byte[Code.CMD_LENGTH + Code.KEY_LENGTH
                            + Code.VALUE_LENGTH_LENGTH + value.length];
                    newReq[0] = Command.FORCE_PUT;

                    System.arraycopy(key, 0, request, Code.CMD_LENGTH, Code.KEY_LENGTH);

                    System.arraycopy(key,0,newReq,Code.CMD_LENGTH,Code.KEY_LENGTH);
                    byte[] valLength = ByteBuffer.allocate(4)
                            .order(ByteOrder.LITTLE_ENDIAN).putInt(value.length).array();
                    System.arraycopy(valLength, 0, request, Code.CMD_LENGTH
                            + Code.KEY_LENGTH, Code.VALUE_LENGTH_LENGTH);

                    System.arraycopy(value, 0, request, Code.CMD_LENGTH + Code.KEY_LENGTH
                            + Code.VALUE_LENGTH_LENGTH, value.length);

                    final InetAddress finalSrcAddr = srcAddr;
                    final int finalSrcPort = srcPort;
                    final InetAddress finalSrcServer = srcServer;
                    final int finalServerPort = serverPort;

                    new Thread() {
                        public void run() {
                            try {
                                server.forwardRequest(uniqueRequestID, newReq, InetAddress.getByAddress(firstNode), Config.validPort[0], finalSrcAddr, finalSrcPort, false, finalSrcServer, finalServerPort);
                                if (!Arrays.equals(only_one, thirdNode)) {
                                	server.forwardRequest(uniqueRequestID, newReq, InetAddress.getByAddress(thirdNode), Config.validPort[0], finalSrcAddr, finalSrcPort, false, finalSrcServer, finalServerPort);
                                }
                            }catch (UnknownHostException uh){
                                System.err.println("PASS_REQUEST:ip address corrupted, dropping message.");
                            }
                        }}.start();
                    break;
                }
                
                // passes it third node if not found
                if(Arrays.equals(only_one,thirdNode)){
                	response = Response.INVALID_KEY;
                	break;
				} else {
                    try {
                        server.forwardRequest(uniqueRequestID, request, InetAddress.getByAddress(thirdNode), Config.validPort[0], srcAddr, srcPort, false, srcServer, serverPort);
                    }catch (UnknownHostException uh){
                        System.err.println("PASS_REQUEST:ip address corrupted, dropping message.");
                    }
                    return;
                }
			} else {
				System.out.println("I am third node!");
                if (value != null) {
                    response = Response.SUCCESS;
                    byte[] newReq = new byte[Code.CMD_LENGTH + Code.KEY_LENGTH
                            + Code.VALUE_LENGTH_LENGTH + value.length];
                    newReq[0] = Command.FORCE_PUT;

                    System.arraycopy(key, 0, request, Code.CMD_LENGTH, Code.KEY_LENGTH);

                    byte[] valLength = ByteBuffer.allocate(4)
                            .order(ByteOrder.LITTLE_ENDIAN).putInt(value.length).array();
                    System.arraycopy(valLength, 0, request, Code.CMD_LENGTH
                            + Code.KEY_LENGTH, Code.VALUE_LENGTH_LENGTH);

                    System.arraycopy(value, 0, request, Code.CMD_LENGTH + Code.KEY_LENGTH
                            + Code.VALUE_LENGTH_LENGTH, value.length);


                    final InetAddress finalSrcAddr = srcAddr;
                    final int finalSrcPort = srcPort;
                    final InetAddress finalSrcServer = srcServer;
                    final int finalServerPort = serverPort;

                        new Thread() {
                            public void run() {
                                try {
                                server.forwardRequest(uniqueRequestID, newReq, InetAddress.getByAddress(firstNode), Config.validPort[0], finalSrcAddr, finalSrcPort, false, finalSrcServer, finalServerPort);
                                server.forwardRequest(uniqueRequestID, newReq, InetAddress.getByAddress(secondNode), Config.validPort[0], finalSrcAddr, finalSrcPort, false, finalSrcServer, finalServerPort);
                                }catch (UnknownHostException uh){
                                    System.err.println("PASS_REQUEST:ip address corrupted, dropping message.");
                                }
                            }}.start();
                    break;
                } else {
                    response = Response.INVALID_KEY;
                    break;
                }
            }
            case Command.PUT:
			if (!isValidKey(request)) {
				response = Response.INVALID_KEY;
				break;
			} else if (!isKeyInRange(request)) {
				if (!routeMsgTo) {
					srcServer = dht.getLocalHost();
					serverPort = server.getPort();
				}
				DHT.Successor s = closestSuccessorTo(request);
				server.forwardRequest(uniqueRequestID, request, s.ip,
						s.udpPort, srcAddr, srcPort, false, srcServer,
						serverPort);
				System.out.println("put out of range.");
				return;
			} else if (table.size() >= maxSize) {
				response = Response.OUT_OF_SPACE;
				break;
			}
			System.out.println("put in range.");

			// checks if the value length matches the actual value length
			byte[] valueLength = new byte[4];
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

                request[0] = Command.FORCE_PUT;

                if (!routeMsgTo) {
					srcServer = dht.getLocalHost();
					serverPort = server.getPort();
				}
                ArrayList<DHT.Successor> firstTwo = dht.firstTwoSuccessor();
                for(DHT.Successor s: firstTwo){
                    server.forwardRequest(uniqueRequestID, request, s.ip,
                            s.udpPort, srcAddr, srcPort, false, srcServer,
                            serverPort);
                }

				response = Response.SUCCESS;
			}

			break;
        case Command.FORCE_PUT:
            byte[] valueLength0 = new byte[4];
            if (request.length < Code.CMD_LENGTH + Code.KEY_LENGTH
                    + Code.VALUE_LENGTH_LENGTH) {
                response = Response.INVALID_VALUE;
                break;
            }
            // copy the val_len_len out
            System.arraycopy(request, Code.CMD_LENGTH + Code.KEY_LENGTH,
                    valueLength0, 0, Code.VALUE_LENGTH_LENGTH);
            // length of value as specified by val_length
            int intValueLength0 = ByteBuffer.wrap(valueLength0)
                    .order(ByteOrder.LITTLE_ENDIAN).getInt();

            // actual length of value
            int actualValueLength0 = request.length - Code.CMD_LENGTH
                    - Code.VALUE_LENGTH_LENGTH - Code.KEY_LENGTH;

            if (intValueLength0 > actualValueLength0
                    || intValueLength0 <= 0
                    || intValueLength0 > Config.MAX_APPLICATION_PAYLOAD
                    - Code.CMD_LENGTH - Code.VALUE_LENGTH_LENGTH) {

            } else {
                byte[] v = new byte[intValueLength0 + Code.VALUE_LENGTH_LENGTH];
                System.arraycopy(request, Code.CMD_LENGTH + Code.KEY_LENGTH, v,
                        0, v.length);
                table.put(parseKey(request), v);
            }
            System.out.println(dht.getLocalHost().getHostName() + " Calling Force_Put");
            return;
		case Command.REMOVE:
			if (!isValidKey(request)) {
				response = Response.INVALID_KEY;
				break;
			} else if (!isKeyInRange(request)) {
				if (!routeMsgTo) {
					srcServer = dht.getLocalHost();
					serverPort = server.getPort();
				}
				DHT.Successor s = closestSuccessorTo(request);
				server.forwardRequest(uniqueRequestID, request, s.ip,
						s.udpPort, srcAddr, srcPort, false, srcServer,
						serverPort);
				return;
			}
			if (table.remove(parseKey(request)) != null) {
				response = Response.SUCCESS;

                request[0] = Command.FORCE_REMOVE;
                ArrayList<DHT.Successor> firstTwo = dht.firstTwoSuccessor();
                if (!routeMsgTo) {
					srcServer = dht.getLocalHost();
					serverPort = server.getPort();
				}
                for(DHT.Successor s: firstTwo){
                    server.forwardRequest(uniqueRequestID, request, s.ip,
                            s.udpPort, srcAddr, srcPort, false, srcServer,
                            serverPort);
                }

			} else {
				response = Response.INVALID_KEY;
			}
            break;
        case Command.FORCE_REMOVE:
            table.remove(parseKey(request));
            System.out.println(dht.getLocalHost().getHostName() + " Calling Force_Remove");
            return;

		case Command.SHUTDOWN:
			// insta kill
			System.exit(0);

			// this wont be run
			response = Response.SUCCESS;
			new Thread() {
				@Override
				public void run() {
					stopMe();
					dht.stopMe();
				}
			}.start();
			break;
		case Command.ARE_YOU_ALIVE:
			response = Response.I_AM_ALIVE;
			break;
		case Command.PASS_REQUEST:
		case Command.RETURN_RESPONSE:
			byte[] parsedRequest = new byte[request.length - 17];

			// source client
			byte[] ip = new byte[4];
			byte[] port = new byte[4];

			// source server
			byte[] sIp = new byte[4];
			byte[] sPort = new byte[4];

			System.arraycopy(request, Code.CMD_LENGTH, ip, 0, 4);
			System.arraycopy(request, Code.CMD_LENGTH + 4, port, 0, 4);

			System.arraycopy(request, Code.CMD_LENGTH + 8, sIp, 0, 4);
			System.arraycopy(request, Code.CMD_LENGTH + 12, sPort, 0, 4);

			System.arraycopy(request, Code.CMD_LENGTH + 16, parsedRequest, 0,
					request.length - 17);

			InetAddress srcSer;
			InetAddress src;
			try {
				srcSer = InetAddress.getByAddress(sIp);
				src = InetAddress.getByAddress(ip);
			} catch (UnknownHostException e) {
				System.err
						.println("PASS_REQUEST:ip address corrupted, dropping message.");
				return;
			}
			int intPort = ByteBuffer.wrap(port).getInt();
			int intSerPort = ByteBuffer.wrap(sPort).getInt();

			if (command == Command.PASS_REQUEST) {
				handleRequest(uniqueRequestID, parsedRequest, src, intPort,
						true, srcSer, intSerPort);
				return;
			} else { // final return to client
				srcAddr = src;
				srcPort = intPort;
				response = parsedRequest[0];
				if (parsedRequest.length > 1) {
					value = new byte[parsedRequest.length - 1];
					System.arraycopy(parsedRequest, 1, value, 0, value.length);
				}
			}
			break;
		case Command.GET_ALL_NODES:
			System.out.println("getallnodes");
			InetAddress thisHost = dht.getLocalHost();
			int thisPort = server.getPort();

			if (!routeMsgTo) {
				srcServer = dht.getLocalHost();
				serverPort = server.getPort();
			} else {
				if (srcServer.equals(dht.getLocalHost())) {
					routeMsgTo = false;
					response = Response.SUCCESS;
					value = new byte[request.length - 1];
					System.arraycopy(request, 1, value, 0, request.length - 1);
					System.out.println("returning nodes");
					break;
				}
			}
			DHT.Successor s = dht.getFirstSuccessor();

			// create new request to be forwarded
			byte[] newRequest = new byte[request.length + 8];
			System.arraycopy(request, 0, newRequest, 0, request.length);

			// copy in the passed in this server IP
			byte[] serverAddress = thisHost.getAddress();
			System.arraycopy(serverAddress, 0, newRequest, request.length, 4);

			// copy over this server Port
			byte[] udpPort = ByteBuffer.allocate(4)
					.putInt(thisPort).array();
			System.arraycopy(udpPort, 0, newRequest, request.length + 4, 4);

			server.forwardRequest(uniqueRequestID, newRequest, s.ip, s.udpPort,
					srcAddr, srcPort, false, srcServer, serverPort);
			return;
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

		if (routeMsgTo) { // final reply to another server
			server.forwardRequest(uniqueRequestID, data, null, 0, srcAddr,
					srcPort, true, srcServer, serverPort);
		} else { // proceed as normal
			server.reply(uniqueRequestID, data, srcAddr, srcPort);
		}
	}

	private boolean isValidKey(byte[] request) {
		if (request.length < Code.CMD_LENGTH + Code.KEY_LENGTH) {
			return false;
		} else {
			return true;
		}
	}

	// get the key from the message (in hex String format)
	private BigInteger parseKey(byte[] request) {
		byte[] key = new byte[Code.KEY_LENGTH];
		System.arraycopy(request, Code.CMD_LENGTH, key, 0, Code.KEY_LENGTH);
		return DHT.positiveBigIntegerHash(key);
	}

	private boolean isKeyInRange(byte[] request) {
		byte[] key = new byte[Code.KEY_LENGTH];
		System.arraycopy(request, Code.CMD_LENGTH, key, 0, Code.KEY_LENGTH);
		return dht.isKeyInRange(key);
	}

	private DHT.Successor closestSuccessorTo(byte[] request) {
		byte[] key = new byte[Code.KEY_LENGTH];
		System.arraycopy(request, Code.CMD_LENGTH, key, 0, Code.KEY_LENGTH);
		return dht.closestSuccessorTo(key);
		// return dht.getFirstSuccessor();
	}
}