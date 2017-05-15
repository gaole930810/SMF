package com.NettyCS;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.*;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.Proto.SecondaryMetaClass;
import com.Proto.SecondaryMetaClass.SecondaryMeta;
import com.Proto.SecondaryMetaClass.SecondaryMeta.FrameInfoGroup;
import com.UtilClass.ConfUtil;
import com.UtilClass.UploadFile;
import com.VMD.HDFSProtocolHandlerFactory;
import com.VMD.VMDProtoUtil;
import com.VMD.XugglerDecompressor;
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
    public static Map<Path, List<FrameInfoGroup>> smf = new ConcurrentHashMap<Path, List<FrameInfoGroup>>();

    public Server(int PORT){
        this.PORT = PORT;
        initialSMFMap();
    }

    /**
     * 日志
     */
    public static final Log LOG = LogFactory.getLog(Server.class);
    
//    private static Logger logger = LoggerFactory.getLogger(Server.class);
    
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
            smf.put(path,sm.getFrameMetaInfoList());
        }catch (IOException e) {
            e.printStackTrace();
        }
        
		return true;
	}
	public static boolean generateSMF(String url){
		Path path =new Path(url);
		URLProtocolManager mgr = URLProtocolManager.getManager();
        if (path.toString().startsWith("hdfs:"))
            mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory());
        IContainer container = IContainer.make();
        container.open(path.toString(), IContainer.Type.READ, null);
        boolean header = false;
        for (int i = 0; i < container.getNumStreams(); i++) {
            IStream stream = container.getStream(i);
            IStreamCoder coder = stream.getStreamCoder();
            if (coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
                continue;
            IPacket packet = IPacket.make();
            Long frameNo = 0L;
            Map<Long, Long> map = new LinkedHashMap<>();
            while (container.readNextPacket(packet) >= 0) {
                if (packet.getStreamIndex() == i) {
                    if (!header) {
                        map.put(frameNo, packet.getPosition());
                        header = true;
                    }
                    frameNo++;
                    if (packet.isKeyPacket()) {
                        map.put(frameNo, packet.getPosition());
                    }
                }
            }
            LOG.debug(map.size());
            SecondaryMeta SM = VMDProtoUtil.genProto(UploadFile.generateSummary(path)
                    , Long.parseLong(ConfUtil.generate().get("dfs.blocksize"))
                    , container.getContainerFormat().getInputFormatShortName()
                    , container.getDuration()
                    , coder.getCodec().getName()
                    , frameNo
                    , map);
            smf.put(path, SM.getFrameMetaInfoList());
            container.close();
            return true;
        }
        container.close();		
		return false;
	}
	public static boolean deleteSMF(String url){
		return false;
	}
	public static long[] getSeqAndIndex(String url,int FrameNo){
		long[] res=new long[2];
		Path path = new Path(url);
//		System.out.println("正在处理：\n"+url+"\n"+FrameNo+" "+res[0]+" "+res[1]);
		List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig = smf.get(path);
        LOG.debug(fig.size());
        res[0] = fig.get(FrameNo).getStartIndex();
        LOG.debug("test_startIndex : " + res[0]);
        res[1] = fig.get(FrameNo).getStartFrameNo();
        LOG.debug("test_StartFrameNo : " + res[1]);
		return res;
	}
    public void bind() {
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
}
