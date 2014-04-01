package com.here.traffic.quality.correlation;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.math3.stat.descriptive.rank.Min;

public class Test {
	public static void main(String[] args){
		new Test().main();
		/*try {
			boolean c=(3+2>5)?true:false;
			System.out.println(c);
			
			double[] x={.12,1231.123,1.23,-123.1};
			Min min=new Min();
			System.out.println(min.evaluate(x));
			
			
			DateFormat simpDateFormat=new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");//must be small letters
			Date systeTimestamp = simpDateFormat.parse("2013-12-12 13:39:46");
			int epochIdx=(systeTimestamp.getHours()*3600+systeTimestamp.getMinutes()*60+systeTimestamp.getSeconds())
					/180*180;
			System.out.println(epochIdx);
		} catch (ParseException e) {
			e.printStackTrace();
		}*/
	}
	
	public void main(){
		long t=System.currentTimeMillis();
		System.out.println(f(10) );
		long diff=System.currentTimeMillis()-t;
		System.out.println((diff+0.0)/1000);
	}
	
	long f(int i){
		int s=0;
		while(i-->0) s+=f(i);
		return Math.max(s, 1);
	}
	
	
}
