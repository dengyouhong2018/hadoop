package com.ct.common.bean;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface DataIn extends Closeable {

    void setPath(String path);

    Object read() throws IOException;

   <T extends Data> List<T> read(Class<T> clazz) throws IOException;
}
