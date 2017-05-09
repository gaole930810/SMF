package com.Test;

import com.NettyCS.Client;
import com.NettyCS.Server;

public class STest {
	public static void main(String[] args){
		new Server(8000).bind();
		return;
	}

}
