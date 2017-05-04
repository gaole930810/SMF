package com.Test;

import com.Proto.VideoSequenceFileRecordClass;
import com.UtilClass.ConfUtil;
import com.VSF.VsfReader;
import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 测试基于VSF方案的分布式读取性能
 * Created by yty on 2016/12/19.
 */
public class ExpEight {
    private static final Log LOG = LogFactory.getLog(ExpEight.class);

    public static void main(String[] args) throws IOException {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);
        long start;
        long end;
        List<Long> t1 = new ArrayList<>();
        List<Long> t2 = new ArrayList<>();
        double t3 = 0.0;
        String basePath = "hdfs://vm1:9000/yty/vsf/";
        String videoSummary = args[0];
        FileSystem fs = FileSystem.get(ConfUtil.generate());
        long fileLen = fs.getFileStatus(new Path(basePath + videoSummary)).getLen();
        for (double i = 0.0; i < 1.0; i += 0.01) {
            start = System.currentTimeMillis();
            VsfReader vsfReader = new VsfReader(basePath + videoSummary);
            SequenceFile.Reader reader = vsfReader.getReader();
            reader.sync((long) (fileLen * i));
            end = System.currentTimeMillis();
            t1.add(end - start);
            IntWritable key = new IntWritable();
            BytesWritable value = new BytesWritable();
            if (reader.next(key, value)) {
                LOG.info(key.get());
                start = System.currentTimeMillis();
                VideoSequenceFileRecordClass.VideoSequenceFileRecord record = VsfReader.transfer(value);
                List<VideoSequenceFileRecordClass.VideoSequenceFileRecord.VideoData> videoDataList = record.getRecordVideoDataList();
                List<BufferedImage> biList = videoDataList.stream()
                        .filter(vd -> (vd.getFrameNo() - record.getStartFrame()) < 50)
                        .map(VideoSequenceFileRecordClass.VideoSequenceFileRecord.VideoData::getFrameData)
                        .map(fd -> {
                            try {
                                BufferedImage bi = ImageIO.read(new ByteArrayInputStream(fd.toByteArray()));
                                return bi;
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            return null;
                        })
                        .collect(Collectors.toList());
                end = System.currentTimeMillis();
                t2.add(end - start);
                int totalSize = videoDataList.stream()
                        .map(VideoSequenceFileRecordClass.VideoSequenceFileRecord.VideoData::getFrameData)
                        .map(ByteString::size)
                        .reduce(Integer::sum)
                        .get();
                t3 += (double) totalSize / (double) record.getSerializedSize();
            }
        }
        LOG.info(t1);
        LOG.info(t2);
        LOG.info(t3);
    }
}
