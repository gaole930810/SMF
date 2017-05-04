package com.VSF;

import com.Proto.VideoSequenceFileRecordClass;
import com.UtilClass.ConfUtil;
import com.UtilClass.MyContainer;
import com.google.protobuf.ByteString;
import com.xuggle.xuggler.IPacket;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;


import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 此类封装了Video Sequence File的写入操作
 * Created by yty on 2016/12/18.
 */
public class VsfWriter {
    private static final Log LOG = LogFactory.getLog(VsfWriter.class);
    private String sourceFileName;
    private String targetFileName;
    private Configuration conf;
    private SequenceFile.Writer writer;
    /*
    t1 read a frame
    t2 decode a frame
    t3 bufferedImage transfer to byte[]
    t4 append to VSF
     */
    private List<Long> t1 = new ArrayList<>();
    private List<Long> t2 = new ArrayList<>();
    private List<Long> t3 = new ArrayList<>();
    private List<Long> t4 = new ArrayList<>();


    public VsfWriter(String sourceFileName, String targetFileName) {
        this.sourceFileName = sourceFileName;
        this.targetFileName = targetFileName;
        this.conf = ConfUtil.generate();
        LOG.debug("the source file name is " + this.sourceFileName);
    }

    public VsfWriter(String sourceFileName, String targetFileName, Configuration conf) {
        this.sourceFileName = sourceFileName;
        this.targetFileName = targetFileName;
        this.conf = conf;
        LOG.debug("the source file name is " + this.sourceFileName);
    }

    public void init() throws IOException {
        SequenceFile.Writer.Option optPath = SequenceFile.Writer.file(new Path(this.targetFileName));
        SequenceFile.Writer.Option optKey = SequenceFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option optValue = SequenceFile.Writer.valueClass(BytesWritable.class);
        SequenceFile.Writer.Option optCom = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD);
        this.writer = SequenceFile.createWriter(conf, optPath, optKey, optValue, optCom);
    }

    public void write() throws IOException {
        int keyNum = 0;
        long frameNumber = 0L;
        IPacket packet;
        BufferedImage bi;
        MyContainer container = new MyContainer(sourceFileName);
        container.start();
        boolean flag = false;
        long start;
        long end;
        List<VideoSequenceFileRecordClass.VideoSequenceFileRecord.VideoData> vdList = new ArrayList<>();
        start = System.currentTimeMillis();
        while ((packet = container.getVideoPacket()) != null) {
            end = System.currentTimeMillis();
            t1.add(end - start);
            start = System.currentTimeMillis();
            while ((bi = container.decodeFrame(packet)) == null) {
                packet = container.getVideoPacket();
            }
            end = System.currentTimeMillis();
            t2.add(end - start);
            if (!flag) {
                flag = true;
            } else {
                flag = false;
                continue;
            }
            if (packet.isKeyPacket()) {
                if (vdList.size() == 0) {
                    addToList(vdList, frameNumber, bi, packet.getPosition());
                } else {
                    start = System.currentTimeMillis();

                    writer.append(new IntWritable(keyNum++), new BytesWritable(Base64.encodeBase64(genRecord(vdList).toByteArray())));
                    end = System.currentTimeMillis();
                    t4.add(end - start);
                    LOG.debug("generate No." + keyNum + " record");
                    vdList = new ArrayList<>();
                    addToList(vdList, frameNumber, bi, packet.getPosition());
                }
            } else {
                addToList(vdList, frameNumber, bi, packet.getPosition());
            }
            frameNumber += 2;
            start = System.currentTimeMillis();
        }
        if (vdList.size() != 0) {
            writer.append(new IntWritable(keyNum), new BytesWritable(genRecord(vdList).toByteArray()));
            vdList = new ArrayList<>();
        }
        container.stop();
        stop();
    }

    private VideoSequenceFileRecordClass.VideoSequenceFileRecord genRecord(List<VideoSequenceFileRecordClass.VideoSequenceFileRecord.VideoData> vdList) {
        long startIndex = vdList.get(0).getFrameIndex();
        long startFrame = vdList.get(0).getFrameNo();
        long endIndex = vdList.get(vdList.size() - 1).getFrameIndex();
        long endFrame = vdList.get(vdList.size() - 1).getFrameNo();
        long recordFrameNumber = vdList.size();
        return VideoSequenceFileRecordClass
                .VideoSequenceFileRecord
                .newBuilder()
                .setStartIndex(startIndex)
                .setStartFrame(startFrame)
                .setEndFrame(endFrame)
                .setEndIndex(endIndex)
                .setRecordFrameNumber(recordFrameNumber)
                .addAllRecordVideoData(vdList)
                .build();
    }

    private void addToList(List<VideoSequenceFileRecordClass.VideoSequenceFileRecord.VideoData> vdList, long fn, BufferedImage bi, long pos) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        long start = System.currentTimeMillis();
        ImageIO.write(bi, "jpg", bout);
        long end = System.currentTimeMillis();
        t3.add(end - start);
        vdList.add(VideoSequenceFileRecordClass
                .VideoSequenceFileRecord
                .VideoData
                .newBuilder()
                .setFrameNo(fn)
                .setFrameData(ByteString.copyFrom(bout.toByteArray()))
                .setFrameIndex(pos)
                .build());
        bout.close();

    }

    private void stop() throws IOException {
        this.writer.close();
        long avgt1 = (t1.stream()
                .reduce((l1, l2) -> l1 + l2)
                .get()) / t1.size();
        long avgt2 = (t2.stream()
                .reduce((l1, l2) -> l1 + l2)
                .get()) / t2.size();
        long avgt3 = (t3.stream()
                .reduce((l1, l2) -> l1 + l2)
                .get()) / t3.size();
        long avgt4 = (t4.stream()
                .reduce((l1, l2) -> l1 + l2)
                .get()) / t4.size();
        LOG.info("average read a frame needs " + avgt1 + " ms");
        LOG.info("average decode a frame needs " + avgt2 + " ms");
        LOG.info("average transfer a frame needs " + avgt3 + " ms");
        LOG.info("average append a frame needs " + avgt4 + " ms");
        LOG.info("total read a frame needs " + avgt1 * t1.size() + " ms");
        LOG.info("total decode a frame needs " + avgt2 * t2.size() + " ms");
        LOG.info("total transfer a frame needs " + avgt3 * t3.size() + " ms");
        LOG.info("total append a frame needs " + avgt4 * t4.size() + " ms");

    }

}
