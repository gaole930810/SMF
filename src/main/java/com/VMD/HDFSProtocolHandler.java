package com.VMD;

import java.io.IOException;

import com.UtilClass.ConfUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.xuggle.xuggler.io.IURLProtocolHandler;

/**
 * Created by yty on 2016/11/30.
 */
public class HDFSProtocolHandler implements IURLProtocolHandler {

    private static final Log LOG = LogFactory.getLog(HDFSProtocolHandler.class);

    private FSDataInputStream fIn;

    private Path pile;

    private Configuration conf;

    private FileSystem fs;

    public HDFSProtocolHandler() {
        this.conf = ConfUtil.generate();
    }

    /**
     * 有参构造方法。
     * 参数是hadoop的配置类，用于后续打开hdfs读取流。
     *
     * @param conf hadoop配置类
     */
    public HDFSProtocolHandler(Configuration conf) {
        this.conf = conf;
    }

    public HDFSProtocolHandler(String input) {
        // TODO Auto-generated constructor stub
        pile = new Path(input);
        conf = new Configuration();
    }

    /* (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#close()
     */
    public int close() {
        // TODO Auto-generated method stub
        try {
            fIn.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return 0;
    }

    /* (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#isStreamed(java.lang.String, int)
     */
    public boolean isStreamed(String arg0, int arg1) {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * 这里我们实现的版本是只读模式的，由于并没有涉及到写的相关操作，因此flag暂时无用。
     * (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#open(java.lang.String, int)
     */
    public int open(String url, int flags) {
        // 这里的我们只处理hdfs的url。如果不是hdfs的url，直接返回-1，代表读取失败。

        LOG.debug("Opening HDFSProtocolHandler with " + url);
        if (url != null && !url.startsWith("hdfs:"))
            return -1;

        if (url != null)
            pile = new Path(url);

        if (pile == null) return -1;
        FileSystem hdfs;
        try {
            hdfs = FileSystem.get(ConfUtil.generate());

        } catch (IOException e) {
            LOG.debug("HDFS client can't build");
            LOG.debug(e.getStackTrace());
            return -2;
        }
        try {
            fIn = hdfs.open(pile);
        } catch (IOException e) {
            LOG.debug("FSDataInputStream can't open");
            return -2;
        }
        LOG.debug("HDFSProtocolHandler opened");

        return 0;
    }

    /* (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#read(byte[], int)
     */
    public int read(byte[] buf, int size) {
        int r = 0;
        try {
            r = fIn.read(buf, 0, size);
            // 老问题，如果一次FSDataInputStream不能读完所有的字节，说明有可能遇到block边界问题
            // 此时会重新读一次。
            if (r < size) {
                int r2 = fIn.read(buf, r, size - r);
                if (r2 >= 0)
                    r += r2;
            }

            // IUrlProtocolHandler wants return value to be zero if end of file
            // is reached
            if (r == -1)
                r = 0;

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            r = -1;
        }
        return r;
    }

    /**
     * HDFSProtocolHandler类的关键方法
     * 能使xuggle能够处理自定义的协议一定要实现seek方法。
     * 因为xuggler封装的底层ffmpeg的相关方法需要依靠seek方法来定位某一个帧
     * 因此实现自定义协议的核心在于，如何实现seek方法。
     * 由于HDFSInputStream已经实现了seek方法，所以相对简单。
     * 但是很多InputStream类不支持seek方法，此时需要自己实现相关方法。
     *
     * @param offset 偏移量
     * @param whence 标志位
     * @return seek后的位置
     */
    public long seek(long offset, int whence) {
        LOG.debug("seeking to " + offset + ", whence = " + whence);
        long pos;
        try {
            FileStatus status = fs.getFileStatus(pile);
            long len = status.getLen();

            switch (whence) {
                case SEEK_CUR:
                    long old_pos = fIn.getPos();
                    fIn.seek(old_pos + offset);
                    pos = old_pos - fIn.getPos();
                    break;
                case SEEK_END:
                    fIn.seek(len + offset);
                    pos = fIn.getPos() - len;
                    break;
                case SEEK_SIZE:
                    pos = len;
                    break;
                case SEEK_SET:
                default:
                    fIn.seek(offset);
                    pos = fIn.getPos();
                    break;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage());
            //e.printStackTrace();
            return -1;
        }
        return pos;
    }

    /* (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#write(byte[], int)
     */
    public int write(byte[] buf, int size) {
        // TODO Auto-generated method stub
        return 0;
    }

}
