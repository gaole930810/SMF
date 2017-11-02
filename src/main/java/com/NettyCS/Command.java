package com.NettyCS;

import java.io.UnsupportedEncodingException;

import io.netty.buffer.ByteBuf;

public class Command {
	public int Type;
	public String[] args;
	public final static int UPLOAD=10;
	public final static int LS=9;
	public final static int GET=8;
	public final static int ADD_VMDRE=7;
	public final static int DET_VMDRE=6;
	public final static int GET_HOST=5;
	public final static int GET_Server=11;
//	public final static int GET_TIME=4;
	public final static int GET_FRAME=3;
	public final static int DELETE=2;
	public final static int GENERATE=1;

	public Command(int Type, String... args) {
		this.Type = Type;
		this.args = args;
	}
	public Command(ByteBuf msg) throws UnsupportedEncodingException {
		byte[] con = new byte[msg.readableBytes()];
		// 将ByteByf信息写出到字节数组
		msg.readBytes(con);
		String[] s = new String(con, "UTF-8").split("\\+");
		this.Type = Integer.parseInt(s[0]);
		if (s.length > 1)
			this.args = new String[s.length - 1];
		for (int i = 1; i < s.length; i++) {
			this.args[i - 1] = s[i];
		}
	}

	public int getCommand() {
		return Type;
	}

	public void setCommand(int command) {
		this.Type = command;
	}

	public String[] getArgs() {
		return args;
	}

	public void setArgs(String[] args) {
		this.args = args;
	}
}
