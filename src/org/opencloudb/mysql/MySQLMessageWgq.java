package org.opencloudb.mysql;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mycat对binary(blob)的处理存在bug.
 * blob如果是png图片，则png的第一个字符-119，转换为utf8，然后再转回来的时候，信息丢失了。
 * 
 * 这种修复是存在很大缺陷的。但是暂时没有找到更好的修复办法。
 * 好的修复需要修改mycat的接口。
 * 
 * 目前的修改策略，将原始的sql的byte[]格式保留在thread local中，然后再需要的时候替换回去。
 * 想通过在byte转string过程中进行处理。但是这个编码还是相当复杂，涉及jdk源码。
 * 
 * 
 * @author wgq
 */
public class MySQLMessageWgq extends MySQLMessage {
	private static final Logger logger = LoggerFactory.getLogger(MySQLMessageWgq.class);

	private static final String _BINARY = "_binary'";
	
	private static ThreadLocal<byte[]> binary = new ThreadLocal<byte[]>() {
		public byte[] initialValue() {
			return null;
		}
	};

	public static byte[] getBinaryBytes() {
		return binary.get();
	}

	private static void setBinaryBytes(byte[] data) {
		binary.set(data);
	}

	private int p0;

	public MySQLMessageWgq(byte[] data) {
		super(data);
	}

	/**
	 * 如果包含binary的信息，则将binary的信息使用base64进行编码。最后再转换成byte[].
	 * 只在FrontendConnection哪里使用是可以的。其他地方使用就是错误的。
	 */
	public String readString(String charset) throws UnsupportedEncodingException {
		this.p0 = position;
		String s0 = super.readString(charset);
		if (hasBinaryData(s0)) {
			byte[] ndata = new byte[data.length-1-p0+1];
			System.arraycopy(data, p0, ndata, 0, ndata.length);
			setBinaryBytes(ndata);
		}
		return s0;
	}

	private boolean hasBinaryData(String s) {
		if (s.indexOf(_BINARY) != -1) {
			return true;
		}
		return false;
	}

	/**
	 * getQueryBytes when writing into database
	 * 
	 * @param query
	 * @param charset
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static byte[] getQueryBytes(String query, String charset) throws UnsupportedEncodingException {
		if (!hasBinary(query)) {
			return query.getBytes(charset);
		} else {
			byte[] data = query.getBytes(charset);
			byte[] orig = getBinaryBytes();
			if (orig != null && byteEqual(data, orig)) {
				return orig;
			} else {
				return data;
			}
		}
	}

	private static int cmplen = 5;

	private static boolean byteEqual(byte[] data, byte[] orig) {
		if (data.length >= cmplen && orig.length >= cmplen) {
			for (int i = 0; i < cmplen; i++) {
				if (data[i] != orig[i]) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * detect binary data,could be done through thread local variable?
	 * 关键是sql语句中包含了怎么处理？
	 * 
	 * @param query
	 * @return
	 */
	private static boolean hasBinary(String query) {
		if (query.indexOf(_BINARY) != -1) {
			return true;
		}
		return false;
	}
}