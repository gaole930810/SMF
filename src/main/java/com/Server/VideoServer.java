package com.Server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * VideoServer是服务器端
 * 提供上传文件的服务
 * 维护文件名和指纹的MAP数据结构
 *
 * Created by yty on 2016/12/13.
 */
public class VideoServer {
    private static final Log LOG = LogFactory.getLog(VideoServer.class);
    private ConcurrentHashMap<String,String> fileDict = new ConcurrentHashMap<>(128);
    public static void main(String[] args) {

        try {
            ServerSocket server = new ServerSocket(55555);
            Thread thread = new Thread(() -> {
                while (true) {
                    try {
                        LOG.info("start listening ...");
                        Socket socket = server.accept();
                        LOG.info("connection build");
                        receiveFile(socket);
                    } catch (IOException e) {
                        LOG.error("Server has fetal error!");
                        e.printStackTrace();
                    }
                }
            }
            );
            thread.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void receiveFile(Socket socket) {
        byte[] inputByte;
        int length;
        DataInputStream dis = null;
        FileOutputStream fos = null;
        String filePath = "";
        try {
            try {
                dis = new DataInputStream(socket.getInputStream());
                File f = new File("D:/temp");
                if (!f.exists()) {
                    f.mkdir();
                }
                /*
                 * 文件存储位置
                 */
                fos = new FileOutputStream(new File(filePath));
                inputByte = new byte[1024];
                System.out.println("开始接收数据...");
                while ((length = dis.read(inputByte, 0, inputByte.length)) > 0) {
                    fos.write(inputByte, 0, length);
                    fos.flush();
                }
                System.out.println("完成接收：" + filePath);
            } finally {
                if (fos != null)
                    fos.close();
                if (dis != null)
                    dis.close();
                if (socket != null)
                    socket.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

