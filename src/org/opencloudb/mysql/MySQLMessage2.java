package org.opencloudb.mysql;

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
public class MySQLMessage2{
	private static final Logger logger = LoggerFactory.getLogger(MySQLMessage2.class);	
	private static final String _BINARY = "_binary'";
	private byte[] data;
	
	public MySQLMessage2(byte[] data, int position, String sql) {
		if (this.hasBinaryData(sql)) {
			this.data = new byte[data.length - position];
			System.arraycopy(data, position, this.data, 0, this.data.length);
		}
	}

	private boolean hasBinaryData(String s) {
		if (s.indexOf(_BINARY) != -1) {
			return true;
		}
		return false;
	}

	public boolean hasBinary() {
		return data != null;
	}

	public byte[] getBytes() {
		return data;
	}
	
}