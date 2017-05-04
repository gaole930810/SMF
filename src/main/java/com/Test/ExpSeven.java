package com.Test;

import com.UtilClass.UploadFile;
import com.VSF.VsfWriter;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.File;
import java.io.IOException;

/**
 * 用于测试单个VSF转换的具体时间开销。
 * Created by yty on 2016/12/19.
 */
public class ExpSeven {
    public static void main(String[] args) throws IOException {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);
        String path = args[0];
        String targetDir = "hdfs://vm1:9000/yty/vsf/";
        String summary = UploadFile.generateSummary(new File(path));
        VsfWriter vsf = new VsfWriter(path, targetDir + summary);
        vsf.init();
        vsf.write();
    }
}
