package com.benjamin.examples.basic;

import org.openjdk.jol.info.ClassLayout;

/**
 * Created with IntelliJ IDEA.
 * User: ywq
 * Date: 2021-02-22
 * Time: 09:09
 * Description: JVM内存相关
 */
public class HeapMemory {
    public static void main(String[] args) {
        Object obj = new Object();
        System.out.println(ClassLayout.parseInstance(obj).toPrintable());
    }
}
