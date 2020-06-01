package com.ct.producer.io;


import com.ct.common.bean.Data;
import com.ct.common.bean.DataIn;

import java.io.*;
import java.util.List;

/**
 * 本地文件数据输入
 */
public class LocalFileDataIn implements DataIn {

    private BufferedReader reader = null;

    public LocalFileDataIn(String path) {
        setPath(path);
    }

    public void setPath(String path){
        try {
           reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Object read() throws IOException {
        return null;
    }


    /**
     * 读取数据，返回数据集合
     * @param clazz
     * @param <T>
     * @return
     * @throws IOException
     */
    public <T extends Data> List<T> read(Class<T> clazz) throws IOException {
        return null;
    }

    public void close() throws IOException {

    }
}
