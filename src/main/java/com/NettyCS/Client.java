package com.NettyCS;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Client {
    /**
     * 日志
     */
	public static final Log LOG = LogFactory.getLog(Server.class);
    public static Logger logger = Logger.getRootLogger();
//    private Logger logger = LoggerFactory.getLogger(Server.class);

    private String HOST;
    private int PORT;

    public Client(String HOST, int PORT) {
        this.HOST = HOST;
        this.PORT = PORT;
    }

    public void connect(Command command){
    	logger.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
    	logger.setLevel(Level.INFO);
        //配置客户端NIO线程组
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY,true)
                    .handler(new ClientInitializer(command));
            //发起异步连接操作
            logger.debug("发起异步连接操作 - start");
            System.out.println("发起异步连接操作 - start");
            
            ChannelFuture channelFuture = bootstrap.connect(HOST,PORT).sync();
            
            logger.debug("发起异步连接操作 - end");
            System.out.println("发起异步连接操作 - end");
            
            
            //等待客户端链路关闭
            logger.debug("等待客户端链路关闭 - start");
            System.out.println("等待客户端链路关闭 - start");
            
            channelFuture.channel().closeFuture().sync();
            
            logger.debug("等待客户端链路关闭 - end");
            System.out.println("等待客户端链路关闭 - end");
            
        } catch (InterruptedException e) {
            logger.error(e.getMessage(),e);
            System.out.println(e.getMessage());
        }finally {
            //关闭
            eventLoopGroup.shutdownGracefully();
        }
    }
}