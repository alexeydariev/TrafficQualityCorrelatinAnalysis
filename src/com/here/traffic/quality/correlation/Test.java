package com.here.traffic.quality.correlation;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.math.plot.utils.Array;

public class Test {
	public static void main(String[] args){
		int [] arr={13,15,34,623,21,1,9,721};
		Arrays.sort(arr, 2,7);
		System.out.println(Arrays.toString(arr));
		
		
		System.out.println("2014-03-20T13:39:20.417z".split("\\.")[0].replace('T', ' '));
		
		Date systeTimestamp=null;
		String time="2014-03-20 12:16:51";
		try{
			DateFormat simpDateFormat=new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");//must be small letters
			systeTimestamp=simpDateFormat.parse(time);//GMT end time
		}catch(Exception ex){
			DateFormat simpDateFormat=new SimpleDateFormat("yyyy-mm-dd hh:mm");//must be small letters
			try{
			systeTimestamp=simpDateFormat.parse(time);//GMT end time
			//System.out.println(fields[14]+" "+line);
			//continue;
			}catch(Exception e){
			}
		}
		int secondsOfDay=CommonUtils.HMSToSeconds(time.split(" ")[1]);
				//systeTimestamp.getHours()*3600+systeTimestamp.getMinutes()*60+systeTimestamp.getSeconds();
		int epochIdx=secondsOfDay/180;
		System.out.println(systeTimestamp+" "+epochIdx);
		
		//new Test().main();
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
