package com.benjamin.examples.basic;

import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: ywq
 * Date: 2021-02-02
 * Time: 17:22
 * Description:
 */
public class ArrayToList {
    public static void main(String[] args) {
        arr2List();
    }

    private static void arr2List() {
        ArrayList<String> keys = new ArrayList<>(10);
        keys.add("你好");
        keys.add("春节好");
        String[] a = keys.toArray(new String[0]);
        for (String s : a) {
            System.out.println(s);
        }
    }
}
