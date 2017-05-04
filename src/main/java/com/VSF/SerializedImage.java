package com.VSF;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * 一个基于BufferedImage序列化的类
 * Created by yty on 2016/12/18.
 */
public class SerializedImage implements Serializable {
    private BufferedImage bi;

    public SerializedImage(BufferedImage bufferedImage) {
        this.bi = bufferedImage;
    }

    public BufferedImage getBi() {
        return bi;
    }

    private void writeObject(ObjectOutputStream s) throws IOException {
        s.writeInt(bi.getWidth());
        s.writeInt(bi.getHeight());
        s.writeInt(bi.getType());

        for (int i = 0; i < bi.getWidth(); i++) {
            for (int j = 0; j < bi.getHeight(); j++) {
                s.writeInt(bi.getRGB(i, j));
            }
        }
        s.flush();
    }

    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
        bi = new BufferedImage(s.readInt(), s.readInt(), s.readInt());
        for (int i = 0; i < bi.getWidth(); i++) {
            for (int j = 0; j < bi.getHeight(); j++) {
                bi.setRGB(i, j, s.readInt());
            }
        }
    }
}
