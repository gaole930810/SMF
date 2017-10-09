package com.NettyCS;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.MemoryUtil.ServerHashUtil;



public class ServerMaster {
    /**
     * 服务端绑定端口号
     */
    private int PORT;
    
    private static String[] serversIP={
    	"172.16.10.101",
    	"172.16.10.102",
    	"172.16.10.103"
    };
    
    
    public ServerMaster(int PORT){
        this.PORT = PORT;        
    }

    /**
     * 日志
     */
    public static final Log LOG = LogFactory.getLog(ServerMaster.class);
    
    public static String findHost(String url){
    	String HOST="127.0.0.1";
    	HOST=serversIP[ServerHashUtil.findServerSeq(url,serversIP.length)];
    	return HOST;
    }

    public void bind() {
    	//调用Zookeeper服务
    	startZookeeperWatch();
    	
/*    	Logger root = Logger.getRootLogger();
    	root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
    	root.setLevel(Level.INFO);*/
        /*
        NioEventLoopGroup是线程池组
                     包含了一组NIO线程,专门用于网络事件的处理
        bossGroup:服务端,接收客户端连接
        workGroup:进行SocketChannel的网络读写
         */
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workGroup = new NioEventLoopGroup();
        try {
            /*
            ServerBootstrap:用于启动NIO服务的辅助类,目的是降低服务端的开发复杂度
             */
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)//配置TCP参数,能够设置很多,这里就只设置了backlog=1024,
                    .childHandler(new ServerMasterInitializer());//绑定I/O事件处理类
            LOG.debug("绑定端口号:" + PORT + ",等待同步成功");
            System.out.println("绑定端口号:" + PORT + ",等待同步成功");
            /*
            bind:绑定端口
            sync:同步阻塞方法,等待绑定完成,完成后返回 ChannelFuture ,主要用于通知回调
             */
            ChannelFuture channelFuture = serverBootstrap.bind(PORT).sync();
            LOG.debug("等待服务端监听窗口关闭");
            System.out.println("等待服务端监听窗口关闭");
            /*
             closeFuture().sync():为了阻塞,服务端链路关闭后才退出.也是一个同步阻塞方法
             */
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
        	LOG.error(e.getMessage(), e);
            System.out.println(e.getMessage());
        } finally {
        	LOG.debug("退出,释放线程池资源");
            System.out.println("退出,释放线程池资源");
            bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
        }
    }
    public void startZookeeperWatch(){
    	//启动一个新线程，通过Zookeeper监控节点状况，及时更新serverIP;
    	
    }
}
