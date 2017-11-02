package com.VMDServiceAPI;

import com.NettyCS.Client;
import com.NettyCS.Command;
import com.NettyCS.Results;

public class VMDClient {
		
	public static Results LS(String url){
		Command command=new Command(Command.LS,url);		
		return new Client().connect(command);
	}
	public static Results GET(String url){
		Command command=new Command(Command.GET,url);		
		return new Client().connect(command);
	}	
	public static Results GET_FRAME(String url,String FrameSeq){
		Command command=new Command(Command.GET_FRAME,url,FrameSeq);		
		return new Client().connect(command);
	}
	public static Results GENERATE(String url){
		Command command=new Command(Command.GENERATE,url);		
		return new Client().connect(command);
	}
	public static Results DELETE(String url){
		Command command=new Command(Command.DELETE,url);		
		return new Client().connect(command);
	}
	//不打算提供上传视频的方法
	public static Results UPLOAD(String url){
		Command command=new Command(Command.UPLOAD,url);		
		return new Client().connect(command);
	}

}
