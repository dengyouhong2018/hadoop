package com.java.test;

public class Signleton {

    private Signleton(){

    }

    private static class SingletonHalder{
        private static Signleton instance = new Signleton();
    }

    public static Signleton getInstance(){
        return SingletonHalder.instance;
    }

}
