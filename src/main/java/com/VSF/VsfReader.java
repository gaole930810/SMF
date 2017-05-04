package com.VSF;

import com.Proto.VideoSequenceFileRecordClass;
import com.UtilClass.ConfUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * 此类用于实现vsf的读取功能
 * Created by yty on 2016/12/19.
 */
public class VsfReader {
    private static final Log LOG = LogFactory.getLog(VsfReader.class);
    private SequenceFile.Reader reader;

    public VsfReader(String path, Configuration conf) throws IOException {
        SequenceFile.Reader.Option fileopt = SequenceFile.Reader.file(new Path(path));
        this.reader = new SequenceFile.Reader(conf, fileopt);
    }

    public VsfReader(String path) throws IOException {
        SequenceFile.Reader.Option fileopt = SequenceFile.Reader.file(new Path(path));
        this.reader = new SequenceFile.Reader(ConfUtil.generate(), fileopt);

    }

    public SequenceFile.Reader getReader() {
        return reader;
    }

    public static VideoSequenceFileRecordClass.VideoSequenceFileRecord transfer(BytesWritable value) throws IOException {
        return VideoSequenceFileRecordClass.VideoSequenceFileRecord.parseFrom(Base64.decodeBase64(value.getBytes()));
    }
}
