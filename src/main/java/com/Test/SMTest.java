package com.Test;

import com.NettyCS.Client;
import com.NettyCS.ServerMaster;

public class SMTest {
	public static void main(String[] args){
		new ServerMaster(8000).bind();
		return;
	}

}
