package com.VMDServiceAPI;

import com.NettyCS.Client;
import com.NettyCS.Command;
import com.NettyCS.Results;

public class VMDServiceManager {

	public static Results GetAllServerInfo(){
		Command command=new Command(Command.LS);		
		return new Client().connect(command);
	}
	public static Results GetRouter(){
		Command command=new Command(Command.LS);		
		return new Client().connect(command);
	}	
}
