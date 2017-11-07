package com.NettyCS;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.MemoryUtil.ServerHashUtil;
import com.Proto.SecondaryMetaClass.SecondaryMeta;
import com.UtilClass.SerializationUtil;



public class ServerMaster implements Watcher {
	private static double memeryLimit=0.8;
    /**
     * 服务端绑定端口号
     */
    private int PORT;
    private static Map<String,JedisSentinelPool> jedisSentinelPools=new HashMap<String, JedisSentinelPool>();
    
    /*
     * Master服务器中保存的数据
     * redis的访问路径
     * 
     */
    private static Map<String,RedisCluster> redisClusters=new HashMap<String, RedisCluster>();
    /*
     * Master服务器中保存的数据
     * server与redis的对照表
     * 
     */
    private static volatile ConcurrentHashMap<String,String> redisAndServer=new ConcurrentHashMap<String,String>();
    
    /*
     * Master服务器中保存的数据
     * Redis的负载表
     */
    private static volatile Map<String,Double> redisLoadBalance=new ConcurrentHashMap<String, Double>();
    
    
    /*
     * Master服务器中保存的数据
     * 路由表
     * 路由信息，包含了VMD的url、redis的name；
     */
    private static volatile Map<String,String> vmdRouter=new ConcurrentHashMap<String, String>();
    
    
    /*
     * zookeeper参数
     */
    private static String groupNode = "sgroup";
    private static ZooKeeper zk;
    private static Stat stat = new Stat();
    
    
    /*
     * 删
     * */
    private static String[] serversIP={
    	"172.16.10.101",
    	"172.16.10.102",
    	"172.16.10.103"
    };
    
    public ServerMaster(){
               
    }
    public ServerMaster(int PORT){
        this.PORT = PORT;
        initialredisClusters();
        initialjedisSentinelPools();
        initialRouterFromRedis();
        downloadBarance();        
    }
    public static void initialjedisSentinelPools() {
		// TODO Auto-generated method stub
    	for (RedisCluster redisCluster : redisClusters.values()) {
			JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(redisCluster.ClusterName,
					redisCluster.RedisClusterIPs, redisCluster.ClusterPassport);
			jedisSentinelPools.put(redisCluster.ClusterName, jedisSentinelPool);
		}
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
    public static void initialRouterFromRedis(){
    	for (RedisCluster redisCluster : redisClusters.values()) {
			JedisSentinelPool jedisSentinelPool = jedisSentinelPools.get(redisCluster.ClusterName);
			Jedis jedis = null;
			try {
				jedis = jedisSentinelPool.getResource();
				// jedis.set("key", "value");
				Set s = jedis.keys("*");
				Iterator it = s.iterator();

				while (it.hasNext()) {
					String key = (String) it.next();
					String regex = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
							+ "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
							+ "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
							+ "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";
					// 判断ip地址是否与正则表达式匹配
					if (key.startsWith("hdfs://vm1:9000")) {
						vmdRouter.put(key, redisCluster.ClusterName);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				jedisSentinelPool.returnBrokenResource(jedis);
			}
		}
    }

    /**
     * 日志
     */
    public static final Log LOG = LogFactory.getLog(ServerMaster.class);
    
    public static String addVMDRE(String url,String HOST){
    	String redisname=redisAndServer.get(HOST);
    	LOG.debug("redisAndServer.get(HOST) for"+HOST+":"+redisname);
    	String re=vmdRouter.putIfAbsent(url, redisname);
    	LOG.debug("vmdRouter.put(url, redisname) for"+HOST+":"+re);
//    	putOnRedis(url,HOST);
    	return re==null?"addVMDRE SUCCESS":"addVMDRE FAILED EXISTED";
    }
    public static String delVMDRE(String url){
    	String re=vmdRouter.remove(url);
 //   	delFromRedis(url);
    	return re==null?"delVMDRE FAILD":"delVMDRE SUCCESS";
    }
    public static Map<String, Map<String, Integer>> getls(String filepath) {
		// TODO Auto-generated method stub
    	Map<String, Map<String, Integer>> re = new HashMap<String, Map<String, Integer>>();
    	for(String redisname:redisClusters.keySet()){
    		Map<String, Integer> mapa=new HashMap<String, Integer>();
    		re.put(redisname, mapa);
    	}
    	for(String url:vmdRouter.keySet()){
    		if(url.startsWith(filepath)){
    			String redisname=vmdRouter.get(url);
        		JedisSentinelPool jedisSentinelPool = jedisSentinelPools.get(redisname);
    			Jedis jedis = null;
    			try {
    				jedis = jedisSentinelPool.getResource();
    				byte[] urlbyte=jedis.get(url.getBytes());
    				if(urlbyte!=null){
    					SecondaryMeta temp=(SecondaryMeta)SerializationUtil.deserialize(urlbyte);
    					re.get(redisname).put(url, temp.getFrameMetaInfoList().size());
    				}                    
    			} catch (Exception e) {
    				e.printStackTrace();
    			} finally {
    				jedisSentinelPool.returnBrokenResource(jedis);
    			}
    		}
    		
    	}   	  	
		return re;
	}
    public static String findServer(String url){
    	String redisname=vmdRouter.get(url);
    	if(redisname!=null){
    		return "VMD EXISTS";
    	}else{
    		//选择哪个redis？
    		redisname=findRedis();
    		//redis对应有哪些server？
    		//选择哪个server？
    		System.out.println("redisname:"+redisname);
    		String Server=findRandomHOST(redisname);
    		System.out.println("Server:"+Server);
    		return Server;
    	}
    }   
    public static String findHost(String url){
    	String redisname=vmdRouter.get(url);
    	if(redisname==null){
    		return "VMD NOT EXISTS";
    	}else{
    		//redis对应有哪些HOST？
    		//选择哪个HOST？
    		String HOST=findRandomHOST(redisname);;
    		return HOST;
    	}
    }
    public static String findRedis(){
    	String redisname="";
    	System.out.println("start to findRedis");
    	Map<String,Double> temp=new HashMap<String,Double>(redisLoadBalance);
    	System.out.println(temp);
    	Set<String> r1=new HashSet<String>();
    	Set<String> r2=new HashSet<String>();    	
    	for(String redis:temp.keySet()){
    		if(temp.get(redis)<=memeryLimit){
    			System.out.println(redis);
    			r1.add(redis);
    		}else if(temp.get(redis)<1){
    			System.out.println(redis);
    			r2.add(redis);
    		}
    	}
    	if(r1.size()>=3){
    		System.out.println("start:rr1.toString()");
    		Object[] rr1= r1.toArray();
    		System.out.println(rr1.toString());
    		redisname=(String)rr1[Math.abs(new Random().nextInt()%rr1.length)];
    	}
    	System.out.println(redisname);
		return redisname;
    }
    public static String findRandomHOST(String redisname){
    	String HOST="";
    	System.out.println("start to findRandomHOST");
    	Map<String,String> temp=new HashMap<String,String>(redisAndServer);
    	System.out.println(temp);
    	Set<String> s1=new HashSet<String>();   	
    	for(String s:temp.keySet()){
    		if(temp.get(s).equals(redisname)){
    			s1.add(s);		
    		}
    	}
    	if(s1.size()>0){
			Object[] ss1=s1.toArray();
    		HOST=(String)ss1[Math.abs(new Random().nextInt()%ss1.length)];
    	}
    	System.out.println(HOST);
		return HOST;
    }
    
    public void bind() {
    	//调用Zookeeper服务
//    	ServerMaster ac=new ServerMaster();
        try {
        	connectZookeeper();
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
                    .option(ChannelOption.SO_BACKLOG, 1024*10)//配置TCP参数,能够设置很多,这里就只设置了backlog=1024,
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
    /**
     * 连接zookeeper服务器
     * 
     * @throws IOException
     * @throws InterruptedException 
     * 
     * @throws KeeperException 
     */
    public void connectZookeeper() throws IOException, KeeperException, InterruptedException {
        zk = new ZooKeeper("172.16.10.101:2181", 5000, this);
        //查看要检测的服务器集群的根节点是否存在，如果不存在，则创建
        if(null==zk.exists("/"+groupNode, false)){
            zk.create("/"+groupNode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        LOG.info("creat a /groupNode:SUCCESSED");
        LOG.info("connectZookeeper:SUCCESSED");
        updateRedisAndServer();
        
    }
    /**
     * 每隔60秒更新一次redis负载
     * 
     */
    private void downloadBarance(){
    	LOG.info("downloadBarance - start");
        new Thread(new  Runnable() {
            public void run() {
                while(true){
                    try {                        
                        Map<String,Double> newRedisLoadBalance=new ConcurrentHashMap<String,Double>();
                        for (String redisname : redisClusters.keySet()) {
                			JedisSentinelPool jedisSentinelPool = jedisSentinelPools.get(redisname);
                			Jedis jedis = null;
                			redis.clients.jedis.Client jedisclient=null;
                			try {
                				jedis = jedisSentinelPool.getResource();
                				String info="";
                                try {   
                                	jedisclient = jedis.getClient();  
                                	jedisclient.info();  
                                    info = jedisclient.getBulkReply();    
                                } finally { 
                                	//关闭流
                                	jedisclient.close();                                       
                                }  
                                String[] strs = info.split("\n");  
                                Long usedMemory = 0L;
                                Long maxMemory = 0L;
                                for (int i = 0; i < strs.length; i++) {  
                                    String s = strs[i]; 
                                    String[] detail = s.split(":");  
                                    if (detail[0].equals("used_memory")) { 
                                    	usedMemory=getIntFromString(detail[1]);
                                        break;  
                                    }  
                                }
                                for (int i = 0; i < strs.length; i++) {  
                                    String s = strs[i];  
                                    String[] detail = s.split(":");  
                                    if (detail[0].equals("maxmemory")) {  
                                    	maxMemory=getIntFromString(detail[1]); 
                                    	maxMemory=maxMemory>0?maxMemory:4L*1024*1024*1024;//默认4G最大内存                                   		
                                        break;  
                                    }  
                                }
                                double lbrate=(double)usedMemory/maxMemory;
                                newRedisLoadBalance.put(redisname, lbrate);              				
                			} catch (Exception e) {
                				e.printStackTrace();
                			} finally {
                				jedisSentinelPool.returnBrokenResource(jedis);
                			}
                        }         
                        redisLoadBalance=newRedisLoadBalance;
                        LOG.info("更新了Redis负载列表:"+redisLoadBalance);
                        Thread.sleep(60000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    
                }
                
            }

			private Long getIntFromString(String input) {
				// TODO Auto-generated method stub
				Long re=0L;
				for(int i=0;i<input.length();i++){
					char t=input.charAt(i);
					if(t>='0'&&t<='9'){
						re=re*10+t-'0';
					}
				}
				return re;
			}
        }).start();
    }
    
   /* private void downloadBarance(){
    	LOG.info("uploadBarance - start");
        new Thread(new  Runnable() {
            public void run() {
                while(true){
                    try {
                        Thread.sleep(60000);
                        Map<String,Double> newRedisLoadBalance=new ConcurrentHashMap<String,Double>();
                        for (RedisCluster redisCluster : redisClusters.values()) {
                			
                                double lbrate=(double)0.1;
                                String redisname=redisCluster.ClusterName;
                                newRedisLoadBalance.put(redisname, lbrate);              				
                        }         
                        redisLoadBalance=newRedisLoadBalance;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    showInitial();
                }

            }
        }).start();
    }*/
    /**
     * 更新服务器列表信息
     * @throws KeeperException
     * @throws InterruptedException
     * @throws UnsupportedEncodingException
     */
    private void updateRedisAndServer() throws KeeperException, InterruptedException, UnsupportedEncodingException {
    	
    	ConcurrentHashMap<String,String> newRedisAndServer=new ConcurrentHashMap<String,String>();
    	List<String> subList=zk.getChildren("/"+groupNode,true);
        for(String subNode:subList){
            //获取每个子节点下关联的服务器负载的信息
            byte[] data=zk.getData("/"+groupNode+"/"+subNode, true, stat);
            String redisname=new String(data,"utf-8");
            newRedisAndServer.put(subNode, redisname);
        }
        // 替换server列表  
        redisAndServer=newRedisAndServer;
        LOG.info("更新了服务器列表:"+redisAndServer);
    }
  /**
     * 更新服务器节点的负载数据
     * @param serverNodePath
     * @throws InterruptedException 
     * @throws KeeperException 
     * @throws UnsupportedEncodingException 
     */
    /*
    private void updateServerLoadBalance(String serverNodePath) throws KeeperException, InterruptedException, UnsupportedEncodingException{
        ServerInfo serverInfo=serverList.get(serverNodePath);
        if(null!=serverInfo){
            //获取每个子节点下关联的服务器负载的信息
            byte[] data=zk.getData(serverInfo.getPath(), true, stat);
            String loadBalance=new String(data,"utf-8");
            serverInfo.setLoadBalance(loadBalance);
            serverList.put(serverInfo.getPath(), serverInfo);
            LOG.info("更新了服务器的负载："+serverInfo);
            LOG.info("更新服务器负载后，服务器列表信息："+serverList);
        }
    }*/

    @Override
    public void process(WatchedEvent event) {
    	LOG.debug("监听到zookeeper事件-----eventType:"+event.getType()+",path:"+event.getPath());
        //集群总节点的子节点变化触发的事件
        if (event.getType() == EventType.NodeChildrenChanged && 
                event.getPath().equals("/" + groupNode)) {
             //如果发生了"/sgroup"节点下的子节点变化事件, 更新server列表, 并重新注册监听  
        	Map<String,RedisCluster> newRedisClusters=new HashMap<String,RedisCluster>(redisClusters);
        	int redisSize=newRedisClusters.size();
        	
        	
            try {
            	updateRedisAndServer();
            } catch (Exception e) {
                e.printStackTrace();
            } 
        }
/*        if (event.getType() == EventType.NodeDataChanged && 
                event.getPath().startsWith("/" + groupNode)) {
             //如果发生了服务器节点数据变化事件, 更新server列表, 并重新注册监听  
            try {
                updateServerLoadBalance(event.getPath());
            } catch (Exception e) {
                e.printStackTrace();
            } 
        }*/
    }
    /**
     * client的工作逻辑写在这个方法中 
     * 此处不做任何处理, 只让client sleep 
     * @throws InterruptedException 
     */
    public void handle() throws InterruptedException{
        Thread.sleep(Long.MAX_VALUE);
    }
/*    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        AppServerMonitor ac=new AppServerMonitor();
        ac.connectZookeeper();
        ac.handle();
    }*/
	
}
