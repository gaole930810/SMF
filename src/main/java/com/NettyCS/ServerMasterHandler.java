package com.NettyCS;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.*;
//import org.jboss.netty.util.internal.ConcurrentHashMap;


import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Handler主要用于对网络事件进行读写操作,是真正的业务类 通常只需要关注 channelRead 和 exceptionCaught 方法
 */
public class ServerMasterHandler extends ChannelInboundHandlerAdapter {

	/**
	 * 日志
	 */
	public static final Log LOG  = LogFactory.getLog(Server.class);
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		/*Logger root = Logger.getRootLogger();
	    root.addAppender(new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
		root.setLevel(Level.INFO);*/
		// ByteBuf,类似于NIO中的ByteBuffer,但是更强大
		ByteBuf reqBuf = (ByteBuf) msg; // msg： 命令+url
		Command command = new Command(reqBuf);
		// 获取请求字符串
		// String req = getReq(reqBuf);

		LOG.debug("From:" + ctx.channel().remoteAddress());
		//System.out.println("From:" + ctx.channel().remoteAddress());

		LOG.debug("服务端收到:" + command.Type);
		//System.out.println("服务端收到:" + command.Type);
		String resStr = "";
		ByteBuf resBuf = null;
		String url;
		switch (command.Type) {
		case Command.LS:
			if (command.args.length != 1) {
				LOG.info("args error!");
			}
			url = command.args[0];
			//添加方法
			Map<String,Map<String,Integer>> temp = ServerMaster.getls(url);
			for(Entry<String,Map<String,Integer>> entry:temp.entrySet()){
				resStr+=entry.getKey()+":\r\n";
				for(Entry<String,Integer> entry2:entry.getValue().entrySet()){
					resStr+=entry2.getKey()+":"+entry2.getValue()+"个I帧\r\n";
				}
			}
			resBuf = getRes(resStr);
			LOG.debug("服务端应答数据:" + resStr);
			//System.out.println("服务端应答数据:" + resStr);
			ctx.write(resBuf);
			break;
		case Command.GET_HOST:
			if (command.args.length != 1) {
				LOG.info("args error!");
			}
			url = command.args[0];
			// 添加方法
			resStr = ServerMaster.findHost(url);

			resBuf = getRes(resStr);
			LOG.debug("服务端master应答数据:\n" + resStr);
			//System.out.println("服务端应答数据:\n" + resStr);
			ctx.write(resBuf);
			break;
		case Command.GET_Server:
			if (command.args.length != 1) {
				LOG.info("args error!");
			}
			url = command.args[0];
			// 添加方法
			resStr = ServerMaster.findServer(url);

			resBuf = getRes(resStr);
			LOG.debug("服务端master应答数据:\n" + resStr);
			//System.out.println("服务端应答数据:\n" + resStr);
			ctx.write(resBuf);
			break;
		case Command.ADD_VMDRE:
			if (command.args.length != 1) {
				LOG.info("args error!");
			}
			url = command.args[0];
			String h=ctx.channel().remoteAddress().toString().split(":")[0];
			// 添加方法
			resStr = ServerMaster.addVMDRE(url,h.substring(1));
			System.out.println("add:response to"+h+":"+resStr);
			resBuf = getRes(resStr);
			LOG.debug("服务端master应答数据:\n" + resStr);
			//System.out.println("服务端应答数据:\n" + resStr);
			ctx.write(resBuf);
			break;
		case Command.DET_VMDRE:
			if (command.args.length != 1) {
				LOG.info("args error!");
			}
			url = command.args[0];
			// 添加方法
			resStr = ServerMaster.delVMDRE(url);
			String hh=ctx.channel().remoteAddress().toString().split(":")[0];
			System.out.println("del:response to"+hh+":"+resStr);
			resBuf = getRes(resStr);
			LOG.debug("服务端master应答数据:\n" + resStr);
			//System.out.println("服务端应答数据:\n" + resStr);
			ctx.write(resBuf);
			break;
		default:
			// 丢弃
			LOG.debug("丢弃");
			//System.out.println("丢弃");
			ReferenceCountUtil.release(msg);
			break;
		}
	}

	/**
	 * 获取发送给客户端的数据
	 *
	 * @param resStr
	 * @return
	 */
	private ByteBuf getRes(String resStr) throws UnsupportedEncodingException {
		byte[] req = resStr.getBytes("UTF-8");
		ByteBuf pingMessage = Unpooled.buffer(req.length);
		// 将字节数组信息写入到ByteBuf
		pingMessage.writeBytes(req);

		return pingMessage;
	}

	/**
	 * 获取请求字符串
	 *
	 * @param buf
	 * @return
	 */
	private String getReq(ByteBuf buf) {
		byte[] con = new byte[buf.readableBytes()];
		// 将ByteByf信息写出到字节数组
		buf.readBytes(con);
		try {
			return new String(con, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		// 将消息发送队列中的消息写入到SocketChannel中发送给对方
		LOG.debug("channelReadComplete");
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		// 发生异常时,关闭 ChannelHandlerContext,释放ChannelHandlerContext 相关的句柄等资源
		LOG.error("exceptionCaught");
		ctx.close();
	}
}
