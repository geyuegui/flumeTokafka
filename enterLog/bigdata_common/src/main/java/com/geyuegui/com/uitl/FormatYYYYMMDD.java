package com.geyuegui.com.uitl;

import java.text.SimpleDateFormat;
import java.util.Date;

public class FormatYYYYMMDD {
    public static String currentTimeFormatYYYYMMDD(){
        Date date=new Date();
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("YYYY-MM-dd");
        return simpleDateFormat.format(date);
    }
}
