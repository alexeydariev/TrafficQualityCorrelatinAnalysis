package com.here.traffic.quality.correlation;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {
	public static void main(String[] args){
		
		try {
			DateFormat simpDateFormat=new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");//must be small letters
			Date systeTimestamp = simpDateFormat.parse("2013-12-12 13:39:46");
			int epochIdx=(systeTimestamp.getHours()*3600+systeTimestamp.getMinutes()*60+systeTimestamp.getSeconds())
					/180*180;
			System.out.println(epochIdx);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		
	}
}
