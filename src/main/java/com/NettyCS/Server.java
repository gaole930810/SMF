package com.NettyCS;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.MemoryUtil.ServerHashUtil;
import com.MemoryUtil.VMDFileUtil;
import com.Proto.SecondaryMetaClass;
import com.Proto.SecondaryMetaClass.SecondaryMeta;
import com.Proto.SecondaryMetaClass.SecondaryMeta.FrameInfoGroup;
import com.UtilClass.ConfUtil;
import com.UtilClass.SerializationUtil;
import com.UtilClass.SummaryUtil;
import com.UtilClass.UploadFile;
import com.VMD.HDFSProtocolHandlerFactory;
import com.VMD.VMDProtoUtil;
import com.VMD.VideoMetaData;
import com.VMD.XugglerDecompressor;
import com.ZookeeperSevice.AppServer;
import com.sun.management.OperatingSystemMXBean;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.io.URLProtocolManager;


public class Server {
    /**
     * 服务端绑定端口号
     */
    private int PORT;
    private String LocalIP="";
    //smf 已废弃
    public static Map<String, List<FrameInfoGroup>> smf = new ConcurrentHashMap<String, List<FrameInfoGroup>>();
    public static String localVMDFilesPath="/home/b8311/Experiment/ExperimentVMD/";
    private static JedisSentinelPool jedisSentinelPool;
    
    /**
     * zookeeper中集群服务器的总节点
     */
    private String groupNode = "sgroup";
    private ZooKeeper zooKeeper;
    /**
     * 服务器创建的节点的路径
     */
    private String serverNodePath="";
    /**
     * 当前服务器的对应的redis
     */
    private static String redisname;
    private static Map<String,RedisCluster> redisClusters=new HashMap<String, RedisCluster>();
    
    
    public Server(int PORT,String IP,String redisname){
        this.PORT = PORT;
        this.LocalIP=IP;
        this.redisname=redisname;
        initialredisClusters();
        initialjedisSentinelPools();
//        initialSMFMap();
//        initialSMFMapFromLocalVMDFile();
    }
    public static void initialjedisSentinelPools() {
		// TODO Auto-generated method stub
    	RedisCluster redisCluster=redisClusters.get(redisname);
			jedisSentinelPool = new JedisSentinelPool(redisCluster.ClusterName,
					redisCluster.RedisClusterIPs, redisCluster.ClusterPassport);
	}
    public static void initialredisClusters(){
    	redisClusters.put("mymaster123", new RedisCluster("mymaster123", new HashSet<String>(), "b8311"));
    	redisClusters.put("mymaster456", new RedisCluster("mymaster456", new HashSet<String>(), "b8311"));
    	redisClusters.put("mymaster789", new RedisCluster("mymaster789", new HashSet<String>(), "b8311"));
  
		redisClusters.get("mymaster123").RedisClusterIPs.add("172.16.10.101:26379");
		redisClusters.get("mymaster123").RedisClusterIPs.add("172.16.10.102:26379");
		redisClusters.get("mymaster123").RedisClusterIPs.add("172.16.10.103:26379");
		redisClusters.get("mymaster456").RedisClusterIPs.add("172.16.10.104:26379");
		redisClusters.get("mymaster456").RedisClusterIPs.add("172.16.10.105:26379");
		redisClusters.get("mymaster456").RedisClusterIPs.add("172.16.10.106:26379");
		redisClusters.get("mymaster789").RedisClusterIPs.add("172.16.10.107:26379");
		redisClusters.get("mymaster789").RedisClusterIPs.add("172.16.10.108:26379");
		redisClusters.get("mymaster789").RedisClusterIPs.add("172.16.10.109:26379");
    }
    public Server(){
    	
    }

    /**
     * 日志
     */
    public static final Log LOG = LogFactory.getLog(Server.class);
    
//    private static Logger logger = LoggerFactory.getLogger(Server.class);
/**
 * * 已废弃的初始化操作    
 * initial VMD from LocalVMDFile on Server Memory    
 * @return boolean
 */
    public static boolean initialSMFMapFromLocalVMDFile(){
    	File vmdfile=new File(localVMDFilesPath);
    	File[] vmdfiles=vmdfile.listFiles();
    	for(File file:vmdfiles){
    		SecondaryMetaClass.SecondaryMeta sm =VMDFileUtil.GenerateMetaFromFile(file.getAbsolutePath());
    		smf.put(sm.getVideoSummary(), sm.getFrameMetaInfoList());
    	}
    	return true;
	}
/**
 * 已废弃的初始化操作    
 * @return
 */
    public static boolean initialSMFMap(){
    	Path path = new Path("hdfs://vm1:9000/yty/video/Test4.rmvb");
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(ConfUtil.generate());
            if (!hdfs.exists(path)) {
                throw new FileNotFoundException("the file doesn't exist !");
            }
            if (hdfs.isDirectory(path)) {
                throw new FileNotFoundException("it is not a file URL");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String summary = UploadFile.generateSummary(path);
        LOG.info(summary);
//        try {
//            if (!hdfs.exists(new Path(ConfUtil.defaultFS + "/yty/meta/" + summary))) {
//                VMDProtoUtil.writeMeta(path);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        try{
            SecondaryMetaClass.SecondaryMeta sm = SecondaryMetaClass.SecondaryMeta.parseFrom(hdfs.open(new Path(ConfUtil.defaultFS + "/yty/meta/" + summary)));
            smf.put(path.toString(),sm.getFrameMetaInfoList());
        }catch (IOException e) {
            e.printStackTrace();
        }
        
		return true;
	}
    
    public static boolean uploadVideo(String url) {
		// TODO Auto-generated method stub
		return false;
	}
    public static SecondaryMeta get(String url) throws IOException{
    	SecondaryMeta SM=getVMDfromRedis(url);
		if(SM==null){
				LOG.error("no VMD of"+url+",FAILED...");
				LOG.error("get:GET FAILED");
			}    	
    	return SM;
    }          
	public static String generateSMF(String url) throws IOException{
		
    	if(existsKeyOnRedis(url)){
			LOG.info("VMD of "+url+"exist,Generate End...");
			LOG.info("GENERATE EXISTS");
			return "GENERATE EXISTS";
		}    	
		SecondaryMeta SM=VideoMetaData.generateViaIndexEntry(url, ConfUtil.generate("vm1", "9000", "vm1"));
		
		if(!putVMDonRedis(url,SM).equals("OK")){
			LOG.error("putVMDonRedis:FAILED");
			LOG.error("GENERATE FAILED");
			return "GENERATE FAILED";
		}
		return "GENERATE SUCCESSED";
	} 	
	public static String deleteSMF(String url){
		if(!existsKeyOnRedis(url)){
			LOG.info("no VMD of"+url+",Delete End...");
			LOG.info("DELETE NOT EXISTS");
			return "DELETE NOT EXISTS";
		}
		Long d=delVMDfromRedis(url);
		if(d!=1&&d!=0){
			LOG.error("delVMDfromRedis:FAILED");
			LOG.error("DELETE FAILED");
			return "DELETE FAILED";
		}
		LOG.info("deleteSMF:SUCCESSED");
		return "DELETE SUCCESSED";
	}
	public static long[] getSeqAndIndex(String url,int FrameNo) throws IOException{
		long[] res=new long[2];
//		System.out.println("正在处理：\n"+url+"\n"+FrameNo+" "+res[0]+" "+res[1]);
//		List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig = smf.get(url);
		
		SecondaryMeta SM=getVMDfromRedis(url);
		if(SM==null){
				    LOG.error("no VMD of"+url+",FAILED...");
					LOG.error("GETSEQANDINDEX FAILED");
					return res;				
			}
		List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig = SM.getFrameMetaInfoList();
		
        LOG.debug(fig.size());
        if(FrameNo<fig.get(0).getStartFrameNo()){
			res[0]=0;
			res[1]=0;
			LOG.info("GETSEQANDINDEX SUCCESSED");
			return res;
		}
        int start = findStart(FrameNo,0,fig.size()-1,fig);
        res[0] = fig.get(start).getStartIndex();
        LOG.debug("test_startIndex : " + res[0]);
        res[1] = fig.get(start).getStartFrameNo();
        LOG.debug("test_StartFrameNo : " + res[1]);
        LOG.info("GETSEQANDINDEX SUCCESSED");
		return res;
	}
	
	//redis上的数据操作
	
	public static Boolean existsKeyOnRedis(String url){
    	//连接redis服务器，如192.168.0.100:6379
		Jedis jedis = null;
		Boolean re=null;
		try {
			jedis = jedisSentinelPool.getResource();  
			re=jedis.exists(url.getBytes());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jedisSentinelPool.returnBrokenResource(jedis);
		}
        return re;
        
        
    } 
    public static String putVMDonRedis(String url,SecondaryMeta SM){
    	Command command=new Command(Command.ADD_VMDRE,url);
    	Results results=uploadVMDInfo(command);
    	LOG.debug("uploadVMDInfo:ADD_VMDRE:"+results.results);
		Jedis jedis = null;
        String re="";
		try {
			jedis = jedisSentinelPool.getResource();
	    	if(results.results.equals("addVMDRE SUCCESS")){
	    		re= jedis.set(url.getBytes(),SerializationUtil.serialize(SM));
	    		LOG.debug("jedis.set:"+re);
	    	}else{
	    		re= "ServerMaster:"+results.results;
	    		LOG.debug(re);
	    		return re;
	    	}
	    	if(!re.equals("OK")){
	    		LOG.debug("!re.equals:"+re);
	    		Command cm=new Command(Command.DET_VMDRE,url);
	        	re+=uploadVMDInfo(cm);
	        	LOG.debug("uploadVMDInfo:DET_VMDRE:"+re);
	    	}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jedisSentinelPool.returnBrokenResource(jedis);
		}
        return re;
    }
    public static SecondaryMeta getVMDfromRedis(String url){
		Jedis jedis = null;
		SecondaryMeta re=null;
		try {
			jedis = jedisSentinelPool.getResource();  
			re=(SecondaryMeta)SerializationUtil.deserialize(jedis.get(url.getBytes()));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jedisSentinelPool.returnBrokenResource(jedis);
		}
        return re;
    }
    public static Long delVMDfromRedis(String url){
		Jedis jedis = null;
		Long re=-1L;
		try {
			jedis = jedisSentinelPool.getResource();  
			re=jedis.del(url.getBytes());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jedisSentinelPool.returnBrokenResource(jedis);
		}
        if(re!=1){
        	return re;
        }
        Command command=new Command(Command.DET_VMDRE,url);
        Results results=uploadVMDInfo(command);
        
        LOG.info(results.results);
        return re;
    }    
	
	public static int findStart(int FrameNo,int s,int e,List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig){
		int start=0;
		SecondaryMetaClass.SecondaryMeta.FrameInfoGroup temp=null;
		Iterator<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> iter=fig.iterator();
		while(iter.hasNext()){
			temp=iter.next();
			if(temp.getStartFrameNo()>FrameNo){
				start--;
				break;
			}
			start++;
		}
		return start;
	}
    public void bind() {
//    	Server Server=new Server();
        try {
			connectZookeeper("172.16.10.101:2181", LocalIP);
			
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	
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
                    .childHandler(new ServerInitializer());//绑定I/O事件处理类
            LOG.debug("绑定端口号:" + PORT + ",等待同步成功");
            //System.out.println("绑定端口号:" + PORT + ",等待同步成功");
            /*
            bind:绑定端口
            sync:同步阻塞方法,等待绑定完成,完成后返回 ChannelFuture ,主要用于通知回调
             */
            ChannelFuture channelFuture = serverBootstrap.bind(PORT).sync();
            LOG.debug("等待服务端监听窗口关闭");
            //System.out.println("等待服务端监听窗口关闭");
            /*
             closeFuture().sync():为了阻塞,服务端链路关闭后才退出.也是一个同步阻塞方法
             */
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
        	LOG.error(e.getMessage(), e);
            System.out.println(e.getMessage());
        } finally {
        	LOG.debug("退出,释放线程池资源");
            //System.out.println("退出,释放线程池资源");
            bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
        }
    }
    
    /**
     * 连接zookeeper服务器，并在集群总结点下创建EPHEMERAL类型的子节点，把服务器名称存入子节点的数据
     * @param zookeeperServerHost
     * @param serverName
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void connectZookeeper(String zookeeperServerHost, String serverName)
            throws IOException, KeeperException, InterruptedException {
         zooKeeper = new ZooKeeper(zookeeperServerHost, 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 啥都不做

            }
        });
        // 先判断sgroup节点是否存在
        String groupNodePath = "/" + groupNode;
        Stat stat = zooKeeper.exists(groupNodePath, false);
        if (null == stat) {
            zooKeeper.create(groupNodePath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        // 将server的地址数据关联到新创建的子节点上 
        serverNodePath=zooKeeper.create(groupNodePath+"/"+serverName, 
        		redisname.getBytes("utf-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        LOG.info("创建了server节点："+serverNodePath);
        LOG.info("connectZookeeper:SUCCESSED " );
        //System.out.println("创建了server节点："+serverNodePath);
        //定时上传服务器的负载
//        uploadBarance();  
    }
    /**
     * 关闭于zookeeper服务器的连接
     * @throws InterruptedException
     */
    public void closeZookeeper() throws InterruptedException{
        if(null!=zooKeeper){
            zooKeeper.close();
        }
    }
    /**
     * 每隔10秒上传一次负载
     * 
     */
    /*private void uploadBarance(){
    	LOG.info("uploadBarance - start");
        new Thread(new  Runnable() {
            public void run() {
                while(true){
                    try {
                        Thread.sleep(10000);
                        loadBalance=getMemory();
                        //loadBalance=new Random().nextInt(100000);
                        String l=String.valueOf(loadBalance);
                        LOG.info("服务器上传可用内存："+loadBalance);
                        //System.out.println("服务器上传可用内存："+loadBalance);
                        zooKeeper.setData(serverNodePath, l.getBytes("utf-8"), -1);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }


            }
        }).start();
    }*/
    public static long getMemory(){
    	OperatingSystemMXBean osmxb = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);  
        //long totalvirtualMemory = osmxb.getTotalPhysicalMemorySize(); // 物理内存  
        return osmxb.getFreePhysicalMemorySize();
    }   
    
    //服务器端主动通知服务器Master更新路由表   
    public static Results uploadVMDInfo(Command command){
    	Results results=new Results();
    	String ServerMasterIP="172.16.10.110";
    	int ServerMasterPORT=8000;
    	 //配置客户端NIO线程组
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY,true)
                    .handler(new ServerZookeeperInitializer(results,command));
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
        LOG.info("inform serverMaster the new VMD:"+results);
//    	System.out.println("uploadVMDInfo - end");
    	return results;
    }
  

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
          System.out.print("请输入服务器名称（如server001）:");
          Scanner scan = new Scanner(System.in);
          String serverName = scan.nextLine();
          Server Server=new Server();
          Server.connectZookeeper("172.16.10.101:2181", serverName);
          while(true){
              System.out.println("请输入您的操作指令(exit 退出系统)：");
              String command = scan.nextLine();
              if("exit".equals(command)){
                  System.out.println("服务器关闭中....");
                  Server.zooKeeper.close();
                  System.exit(0);
                  break;
              }else{
                  continue;
              }
          }
    }
	
    
}
