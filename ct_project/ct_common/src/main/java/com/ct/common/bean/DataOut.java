package com.ct.common.bean;


import java.io.Closeable;

public interface DataOut extends Closeable {
    void setPath(String path);
}
