package com.Test;

import com.UtilClass.ConfUtil;
import com.UtilClass.UploadFile;
import com.VSF.VsfWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 验证VSF写入和读取的相关功能。
 * Created by yty on 2016/12/18.
 */
public class ExpSix {
    private static final Log LOG = LogFactory.getLog(ExpSix.class);

    public static void main(String[] args) throws IOException {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);
        List<Long> list =  new ArrayList<>();
        int dirnum = Integer.parseInt(args[0]);
        List<String> dirs = new ArrayList<>();
        for (int i = 0; i < dirnum; i++) {
            dirs.add(args[i + 1]);
        }
        for (String path : dirs) {
            calc(new File(path),list);
        }
        System.out.println(list);
    }

    public static void calc(File dir,List<Long> list) throws IOException {
        String sourceDir = dir.toString();
        String targetDir = "hdfs://vm1:9000/yty/vsf/";
        File rootDir = new File(sourceDir);
        File[] dirs = rootDir.listFiles();
        FileSystem fs = FileSystem.get(ConfUtil.generate());
        long start;
        long end;
        File[] files = dir.listFiles();
        long total = 0;
        long totalSize = 0;
        for (File file : files) {
            String summary = UploadFile.generateSummary(file);
            start = System.currentTimeMillis();
            VsfWriter vsf = new VsfWriter(file.toString(), targetDir + summary);
            vsf.init();
            LOG.info("-------------------------------------");
            vsf.write();
            end = System.currentTimeMillis();
            total += end - start;
            totalSize += fs.getFileStatus(new Path(targetDir + summary)).getLen();
        }
        LOG.info("Directory " + dir.getName() + " average using " + (total / files.length) + " ms");
        LOG.info("Directory " + dir.getName() + " average using " + (totalSize / files.length / 1024) + " KB");
        list.add(total / files.length);
        list.add(totalSize / files.length / 1024);
    }
}
