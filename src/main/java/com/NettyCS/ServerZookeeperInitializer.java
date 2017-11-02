package com.NettyCS;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class ServerZookeeperInitializer extends ChannelInitializer<SocketChannel> {
	public Results results;
	public Command command;
	public ServerZookeeperInitializer(Results results,Command command){
		this.results=results;
		this.command=command;
	}
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new ServerZookeeperHandler(results,command));
    }
}
