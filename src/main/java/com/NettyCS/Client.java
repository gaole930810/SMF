package com.NettyCS;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.*;

import com.MemoryUtil.ServerHashUtil;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Client {
    /**
     * 日志
     */
	public static final Log LOG = LogFactory.getLog(Server.class);
    
//    private Logger logger = LoggerFactory.getLogger(Server.class);

    public  Results results=new Results();
    private String HOST="127.0.0.1";
    private int PORT=8000;
    /*private String[] serversIP={
    	"172.16.10.101",
    	"172.16.10.102",
    	"172.16.10.103"
    };*/

    public Client() {                
    }

    public Results connect(Command command){
    	if(command.getCommand()==Command.LS){
    		results=getLs(command);
    		return results;
    	}
    	if(command.getCommand()==Command.DELETE||
    			command.getCommand()==Command.GET||
    			command.getCommand()==Command.GET_FRAME
    			){
    		HOST=getHost(command);
        	if(HOST.equals("VMD NOT EXISTS")){
        		results.results="VMD NOT EXISTS";
        		return results;
        	}
        	return doAfterFind(command);
    	}
    	if(command.getCommand()==Command.GENERATE){
    		System.out.println("GENERATE");
    		HOST=getServer(command);
    		if(HOST.equals("VMD EXISTS")){
    			results.results="VMD EXISTS";
    			return results;
    		}
    		System.out.println(HOST);
    		return doAfterFind(command);
    	}
    	return results;
    	
    }
    public Results doAfterFind(Command command){
    	/*Logger root = Logger.getRootLogger();
    	root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
    	root.setLevel(Level.INFO);*/
        //配置客户端NIO线程组
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(1024*1024))
                    .option(ChannelOption.TCP_NODELAY,true)
                    .handler(new ClientInitializer(command,results));
            //发起异步连接操作
            LOG.debug("发起异步连接操作 - start");
            //System.out.println("发起异步连接操作 - start");
            
            ChannelFuture channelFuture = bootstrap.connect(HOST,PORT).sync();
            
            LOG.debug("发起异步连接操作 - end");
            //System.out.println("发起异步连接操作 - end");
            
            
            //等待客户端链路关闭
            LOG.debug("等待客户端链路关闭 - start");
            //System.out.println("等待客户端链路关闭 - start");
            
            channelFuture.channel().closeFuture().sync();
            
            LOG.debug("等待客户端链路关闭 - end");
            //System.out.println("等待客户端链路关闭 - end");
            
        } catch (InterruptedException e) {
        	LOG.error(e.getMessage(),e);
            System.out.println(e.getMessage());
        }finally {
            //关闭
            eventLoopGroup.shutdownGracefully();
        }
        return results;
    }
    public Results getLs(Command command) {
		// TODO Auto-generated method stub
    	Results Ls=new Results();
    	String ServerMasterIP="172.16.10.110";
    	int ServerMasterPORT=8000;
    	 //配置客户端NIO线程组
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY,true)
                    .handler(new ClientZookeeperInitializer(Ls,command));
            //发起异步连接操作
            LOG.debug("发起异步连接操作 - start");
            //System.out.println("发起异步连接操作 - start");
            
            ChannelFuture channelFuture = bootstrap.connect(ServerMasterIP,ServerMasterPORT).sync();
            
            LOG.debug("发起异步连接操作 - end");
            //System.out.println("发起异步连接操作 - end");
            
            
            //等待客户端链路关闭
            LOG.debug("等待客户端链路关闭 - start");
            //System.out.println("等待客户端链路关闭 - start");
            
            channelFuture.channel().closeFuture().sync();
            
            LOG.debug("等待客户端链路关闭 - end");
            //System.out.println("等待客户端链路关闭 - end");
            
        } catch (InterruptedException e) {
        	LOG.error(e.getMessage(),e);
            System.out.println(e.getMessage());
        }finally {
            //关闭
            eventLoopGroup.shutdownGracefully();
        }
        LOG.info("getLs from  serverMaster:SUCCESS");
    	return Ls;
	}

    public String getHost(Command command){
    	Results HostIP=new Results();
    	Command GetHost=new Command(Command.GET_HOST,command.args[0]);
    	String ServerMasterIP="172.16.10.110";
    	int ServerMasterPORT=8000;
    	 //配置客户端NIO线程组
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY,true)
                    .handler(new ClientZookeeperInitializer(HostIP,GetHost));
            //发起异步连接操作
            LOG.debug("发起异步连接操作 - start");
            //System.out.println("发起异步连接操作 - start");
            
            ChannelFuture channelFuture = bootstrap.connect(ServerMasterIP,ServerMasterPORT).sync();
            
            LOG.debug("发起异步连接操作 - end");
            //System.out.println("发起异步连接操作 - end");
            
            
            //等待客户端链路关闭
            LOG.debug("等待客户端链路关闭 - start");
            //System.out.println("等待客户端链路关闭 - start");
            
            channelFuture.channel().closeFuture().sync();
            
            LOG.debug("等待客户端链路关闭 - end");
            //System.out.println("等待客户端链路关闭 - end");
            
        } catch (InterruptedException e) {
        	LOG.error(e.getMessage(),e);
            System.out.println(e.getMessage());
        }finally {
            //关闭
            eventLoopGroup.shutdownGracefully();
        }
        LOG.info("getHost from  serverMaster:SUCCESS");
    	return HostIP.results;
    }
	public String getServer(Command command){
    	Results ServerIP=new Results();
    	Command GetServer=new Command(Command.GET_Server,command.args[0]);
    	String ServerMasterIP="172.16.10.110";
    	int ServerMasterPORT=8000;
    	 //配置客户端NIO线程组
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY,true)
                    .handler(new ClientZookeeperInitializer(ServerIP,GetServer));
            //发起异步连接操作
            LOG.debug("发起异步连接操作 - start");
            //System.out.println("发起异步连接操作 - start");
            
            ChannelFuture channelFuture = bootstrap.connect(ServerMasterIP,ServerMasterPORT).sync();
            
            LOG.debug("发起异步连接操作 - end");
            //System.out.println("发起异步连接操作 - end");
            
            
            //等待客户端链路关闭
            LOG.debug("等待客户端链路关闭 - start");
            //System.out.println("等待客户端链路关闭 - start");
            
            channelFuture.channel().closeFuture().sync();
            
            LOG.debug("等待客户端链路关闭 - end");
            //System.out.println("等待客户端链路关闭 - end");
            
        } catch (InterruptedException e) {
        	LOG.error(e.getMessage(),e);
            System.out.println(e.getMessage());
        }finally {
            //关闭
            eventLoopGroup.shutdownGracefully();
        }
        LOG.info("getServer from  serverMaster:SUCCESS");
    	return ServerIP.results;
    }
}