package com.matei.eece411.util;

/**
 * Various static routines to help with strings
 */
public class StringUtils {

	public static String byteArrayToHexString(byte[] bytes) {
		StringBuffer buf = new StringBuffer();
		String str;
		int val;

		for (int i = 0; i < bytes.length; i++) {
			val = ByteOrder.ubyte2int(bytes[i]);
			str = Integer.toHexString(val);
			while (str.length() < 2)
				str = "0" + str;
			buf.append(str);
		}
		return buf.toString().toUpperCase();
	}
}
