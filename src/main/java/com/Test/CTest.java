package com.Test;

import com.NettyCS.Client;
import com.NettyCS.Command;
import com.NettyCS.Server;

public class CTest {
	public static void main(String[] args){
		Command command=new Command(Command.GET_FRAME,"hdfs://vm1:9000/yty/video/Test4.rmvb",String.valueOf(1));
//		Command command=new Command(Command.GET_TIME);
//		Command command=new Command(Command.GENERATE,"D://filepath");
		new Client("127.0.0.1", 8000).connect(command);
		return;
	}
}
