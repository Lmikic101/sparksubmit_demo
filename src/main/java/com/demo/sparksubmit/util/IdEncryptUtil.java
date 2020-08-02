package com.demo.sparksubmit.util;

import java.util.Base64;

public class IdEncryptUtil {

    public static String encrypt(String userid){
        return userid.toUpperCase();
    }
    
    public static String decrypt(String userid){
        return userid.toLowerCase();
    }
}
