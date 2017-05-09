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

/**
 * Handler主要用于对网络事件进行读写操作,是真正的业务类 通常只需要关注 channelRead 和 exceptionCaught 方法
 */
public class ServerHandler extends ChannelInboundHandlerAdapter {

	/**
	 * 日志
	 */
	public static final Log LOG = LogFactory.getLog(Server.class);
	public static Logger logger = Logger.getRootLogger();
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		logger.addAppender(new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
		logger.setLevel(Level.INFO);
		// ByteBuf,类似于NIO中的ByteBuffer,但是更强大
		ByteBuf reqBuf = (ByteBuf) msg; // msg： 命令+url
		Command command = new Command(reqBuf);
		// 获取请求字符串
		// String req = getReq(reqBuf);

		logger.debug("From:" + ctx.channel().remoteAddress());
		System.out.println("From:" + ctx.channel().remoteAddress());

		logger.debug("服务端收到:" + command.Type);
		System.out.println("服务端收到:" + command.Type);
		String resStr;
		ByteBuf resBuf;
		String url;
		switch (command.Type) {

		case Command.GET_TIME:
			String timeNow = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(new Date());
			resStr = "当前时间:" + timeNow;

			// 获取发送给客户端的数据
			resBuf = getRes(resStr);

			logger.debug("服务端应答数据:\n" + resStr);
			System.out.println("服务端应答数据:\n" + resStr);
			ctx.write(resBuf);
			break;
		case Command.GET_FRAME:
			if (command.args.length != 2) {
				logger.info("args error!");
			}
			url = command.args[0];
			int FrameNo = Integer.parseInt(command.args[1]);			
			logger.debug("请求帧号：" + FrameNo);
			System.out.println("请求帧号：" + FrameNo);
			long res1 = 0;
			long res2 = 0;
			//添加方法
			long[] res = Server.getSeqAndIndex(url, FrameNo);
			res1=res[0];
			res2=res[1];			
			resStr = String.valueOf(res1) + " " + String.valueOf(res2);
			resBuf = getRes(resStr);
			logger.debug("服务端应答数据:" + resStr);
			System.out.println("服务端应答数据:" + resStr);
			ctx.write(resBuf);
			break;
		case Command.DELETE:
			if (command.args.length != 1) {
				logger.info("args error!");
			}
			url = command.args[0];
			// 添加方法
			Server.deleteSMF(url);

			resStr = "DELETE SUCCESS";
			resBuf = getRes(resStr);
			logger.debug("服务端应答数据:\n" + resStr);
			System.out.println("服务端应答数据:\n" + resStr);
			ctx.write(resBuf);
			break;
		case Command.GENERATE:
			if (command.args.length != 1) {
				logger.info("args error!");
			}
			url = command.args[0];
			// 添加方法
			if(Server.generateSMF(url)==true)
			resStr = "GENERATE SUCCESS";
			else
				resStr = "GENERATE FAILED";
			resBuf = getRes(resStr);
			logger.debug("服务端应答数据:\n" + resStr);
			System.out.println("服务端应答数据:\n" + resStr);
			ctx.write(resBuf);
			break;
		default:
			// 丢弃
			logger.debug("丢弃");
			System.out.println("丢弃");
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
		logger.debug("channelReadComplete");
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		// 发生异常时,关闭 ChannelHandlerContext,释放ChannelHandlerContext 相关的句柄等资源
		logger.error("exceptionCaught");
		ctx.close();
	}
}
