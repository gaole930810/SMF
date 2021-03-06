package com.Test;

import com.UtilClass.ConfUtil;
import com.VMD.VideoMetaData;

import java.io.IOException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * 测试两种生成二级元数据的方法是否正确。
 * Created by yty on 17-6-21.
 */
public class GenerateVMDTest {
    public static void main(String[] args) throws IOException {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);
        String path=null;
        //这个是基于本地文件的测试
        //path = "/home/yty/video/test1.mov";
        //VideoMetaData.print(VideoMetaData.generateViaIndexEntry(path, ConfUtil.generate()));

        //这个是基于HDFS文件的测试
        path = "hdfs://vm1:9000/gl/780.mp4";
        VideoMetaData.print
                (VideoMetaData.generateViaKeyPacket//generateViaIndexEntry
                        (path, ConfUtil.generate("vm1", "9000", "vm1")));


    }
}
