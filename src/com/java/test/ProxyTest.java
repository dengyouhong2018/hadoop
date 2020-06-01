package com.java.test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ProxyTest {
    public static void main(String[] args) {
        Huamn superMan = new superMan();
        Huamn proxyTest = new ProxyObject(superMan);

        proxyTest.eat();
    }
}

interface Huamn{
    void eat();
}

class superMan implements Huamn{

    public superMan() {
    }

    @Override
    public void eat() {
        System.out.println("我爱吃🐟");
    }
}

// 静态代理
class ProxyObject implements Huamn{

    public Huamn huamn;

    public ProxyObject(Huamn huamn) {
        this.huamn = huamn;
    }

    @Override
    public void eat() {
        System.out.println("---------------");
        huamn.eat();
        System.out.println("---------------");
    }
}



class Dynamicproxy{

    public static void getinstance(Object obj){
        Proxy.newProxyInstance(obj.getClass().getClassLoader(), obj.getClass().getInterfaces(),
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        return null;

                    }
                });
    }

}