package com.here.traffic.quality.correlation.ds.v3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class MarketV3 {
	public String marketName;
	public String date;
	
	public HashMap<String, XYMetrics> conditionResults;
	public static HashSet<String> allConditions=new HashSet<String>();
	
	
	public MarketV3(String marketName, String date){
		this.marketName=marketName;
		this.date=date;
		conditionResults=new HashMap<String, XYMetrics>();
	}
	
	public MarketV3(String[] fields){
		marketName=fields[0];
		date=fields[1];
		conditionResults=new HashMap<String, XYMetrics>();
		int idx=2;
		while(idx<fields.length){
			XYMetrics xyMetrics=new XYMetrics();
			conditionResults.put(fields[idx], xyMetrics);
			allConditions.add(fields[idx]);
			
			xyMetrics.avgCoverage=Double.parseDouble(fields[idx+1]);
			xyMetrics.avgRMSE=Double.parseDouble(fields[idx+2]);
			xyMetrics.avgTmcCnt=Double.parseDouble(fields[idx+3]);
			//xyMetrics.coverages=new ArrayList<Double>(Integer.parseInt(fields[idx+4]));
			idx+=5;
		}
	}
	
	public static String getHeader(){
		String header="market,date";
		for(String cond: allConditions){
			header+=","+cond+","+XYMetrics.getHeader();
		}
		return header;
	}
	
	
	public String toString(){
		String ret=marketName+","+date;
		for(String condition: allConditions){
			if(conditionResults.containsKey(condition)) ret+=","+condition+","+conditionResults.get(condition);
			else ret+=","+condition+","+"-1,-1,-1,-1";
		}
		return ret;
	}
}



