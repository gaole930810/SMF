package com.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import com.NettyCS.Client;
import com.NettyCS.Command;
import com.NettyCS.Server;

public class CTest {
	public static void main(String[] args){
		test_connect_NUM(50,50);
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
                		test_GET_FRAME();                        
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
	
	public static void test_GET_FRAME(){
		Command command=new Command(Command.GET_FRAME,"hdfs://vm1:9000/yty/video/Test4.rmvb",String.valueOf(1));
		new Client("127.0.0.1", 8000).connect(command);
		return;
	}
	public static void test_GET_TIME(){
		Command command=new Command(Command.GET_TIME);
		new Client("127.0.0.1", 8000).connect(command);
		return;
	}
	public static void test_GENERATE(){
		Command command=new Command(Command.GENERATE,"D://filepath");
		new Client("127.0.0.1", 8000).connect(command);
		return;
	}	
}
