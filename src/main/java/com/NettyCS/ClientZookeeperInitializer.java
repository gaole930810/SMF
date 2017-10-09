package com.NettyCS;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class ClientZookeeperInitializer extends ChannelInitializer<SocketChannel> {
	public Results results;
	public Command command;
	public ClientZookeeperInitializer(Results results,Command command){
		this.results=results;
		this.command=command;
	}
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new ClientZookeeperHandler(results,command));
    }
}