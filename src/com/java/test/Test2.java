package com.java.test;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Test2 {

    public static void main(String[] args) {

        FileReader fr = null;
        FileWriter fw = null;

        try {
            File file1 = new File("hello1.txt");
            File file2 = new File("hello2.txt");

            fr = new FileReader(file1);
            fw = new FileWriter(file2);

            char[] cubf = new char[5];
            int len;

            if( (len = fr.read(cubf)) != -1 ){
                fw.write(cubf,0,len);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            if(fw!=null){
                try {
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if(fr!=null){
                try {
                    fr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }


    }

}
