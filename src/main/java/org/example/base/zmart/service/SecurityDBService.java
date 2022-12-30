package org.example.base.zmart.service;

import java.util.Date;

public class SecurityDBService {
    public static void saveRecord(Date date, String employeeId, String item) {
        System.out.println("Warning!! Found potential problem !! Saving transaction on "+date+" for "+employeeId+" item "+ item);
    }
}
