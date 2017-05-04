package com.VMD;

/**
 * 目的是生成一个HDFSProtocolHandler的实体类。
 * Created by yty on 2016/11/30.
 */

import com.UtilClass.ConfUtil;
import org.apache.hadoop.conf.Configuration;

import com.xuggle.xuggler.io.IURLProtocolHandler;
import com.xuggle.xuggler.io.IURLProtocolHandlerFactory;


public class HDFSProtocolHandlerFactory implements IURLProtocolHandlerFactory {

    private Configuration conf;

    public HDFSProtocolHandlerFactory() {
        this.conf = ConfUtil.generate();
    }


    public HDFSProtocolHandlerFactory(Configuration conf) {
        this.conf = conf;
    }

    public IURLProtocolHandler getHandler(String protocol, String url, int flags) {
        if (protocol.equals("hdfs")) {
            return new HDFSProtocolHandler(conf);
        }
        return null;
    }

}
