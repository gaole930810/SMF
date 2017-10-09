package com.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import com.NettyCS.Client;
import com.NettyCS.Command;
import com.NettyCS.Results;
import com.NettyCS.Server;

public class CTest {
	private static String url="hdfs://vm1:9000/yty/video/Test4.rmvb";
	public static void main(String[] args){
//		test_connect_NUM(50,50);
		url=args[1];
//		if(args[0].equals("GETTIME")) test_GET_TIME(url).print();
		if(args[0].equals("DELETE")) test_DELETE(url).print();
		if(args[0].equals("GETFRAME"))test_GET_FRAME(url,args[2]).print();
		if(args[0].equals("GENERATE"))test_GENERATE(url).print();
		return;
    }
		

	public static void test_connect_NUM(int thread_num,int client_num){
		ExecutorService exec = Executors.newCachedThreadPool();
        // thread_num个线程可以同时访问
//        final Semaphore semp = new Semaphore(thread_num);
        // 模拟client_num个客户端访问
        for (int index = 0; index < client_num; index++) {
            final int NO = index;
            Runnable run = new Runnable() {
                public void run() {
                    try {
                        // 获取许可
//                        semp.acquire();                       
                        System.out.println("Thread并发事情>>>"+ NO);                                               
                		test_GET_FRAME(url,"1");                        
//                        semp.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            exec.execute(run);
        }
        // 退出线程池
        exec.shutdown();
        return;
	}
	
	public static Results test_GET_FRAME(String url,String FrameSeq){
		Command command=new Command(Command.GET_FRAME,url,FrameSeq);		
		return new Client().connect(command);
	}
/*	public static Results test_GET_TIME(String url){
		Command command=new Command(Command.GET_TIME);		
		return new Client(url).connect(command);
	}*/
	public static Results test_GENERATE(String url){
		Command command=new Command(Command.GENERATE,url);		
		return new Client().connect(command);
	}
	public static Results test_DELETE(String url){
		Command command=new Command(Command.DELETE,url);		
		return new Client().connect(command);
	}
}
