package com.here.traffic.quality.correlation.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;


public class MarketV2 {
	public String name;
	
	public String date;
	public HashSet<String> tmcs;
	public HashSet<String> tables; //all tables of a market 
	
	public double miles;
	
	public MarketV2(String market, HashSet<String> tmcs){
		name=market;
		this.tmcs=tmcs;
		for(String tmc: tmcs){
			tables.add(tmc.substring(0, 3));
		}
	}
	
	public ArrayList<Integer> getBatchesIdxOfRawProbeFiles(){
		ArrayList<Integer> batchesOfRawProbeFiles=new ArrayList<Integer>();
		try{
			for(String table: tables){
				int tableID=Integer.parseInt(table);
				batchesOfRawProbeFiles.add((tableID-100)/3);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return batchesOfRawProbeFiles;
	}
	
	
	//public Conditon conditon;
	public XAxisMetric densityMetrics;
	public HashMap<String, YAxisMetric> qualityMetrics;
	
	public MarketV2(String market, String date){
		name=market;
		this.date=date;
		tmcs=new HashSet<String>();
		tables=new HashSet<String>();
		qualityMetrics=new HashMap<String, YAxisMetric>();
		densityMetrics=new XAxisMetric();
	}
	
	public MarketV2(String[] fields){
		name=fields[0];
		date=fields[1];
		densityMetrics=new XAxisMetric();
			densityMetrics.avgProbeCntPerTMC=Double.parseDouble(fields[2]);
			densityMetrics.avgVehicleCntPerTMC=Double.parseDouble(fields[3]);
			densityMetrics.noOfTMCsWithProbeCntOverThreshold=Double.parseDouble(fields[4]);
			densityMetrics.noOfTMCsWithVehicleCntOverThreshold=Double.parseDouble(fields[5]);
		qualityMetrics=new HashMap<String, YAxisMetric>();
		int idx=6;
		while(idx<fields.length-4){
			qualityMetrics.put(fields[idx], new YAxisMetric(Double.parseDouble(fields[idx+1])));
			ConditionV2.allConditonV2s.add(fields[idx]);
			idx+=2;
		}
		tmcs=new HashSet<String>();
			densityMetrics.sumOfProbes=Double.parseDouble(fields[idx+1]);
			densityMetrics.sumOfVehicles=Double.parseDouble(fields[idx+2]);
		miles=Double.parseDouble(fields[idx+3]);		
	}
	
	public static String getHeader(){
		String header="market,date,avgProbeCntPerTMC,avgVehicleCntPerTMC" 
				+",noOfTMCsWithProbeCntOverThrshold,noOfTMCsWithVehicleCntOverThreshold";
		for(String cond: ConditionV2.allConditonV2s){
			header+=",Condition,qualityScore";
		}
		header+=",noOfTMCs,totalCntOfProbes,totalCntOfVehicles,totalMiles";
		return header;
	}
	
	public String toString(){
		String marketString=name+","+date;
		marketString+=","+String.format("%.3f", densityMetrics.avgProbeCntPerTMC)
				+","+String.format("%.3f", densityMetrics.avgVehicleCntPerTMC)
				+","+densityMetrics.noOfTMCsWithProbeCntOverThreshold
				+","+densityMetrics.noOfTMCsWithVehicleCntOverThreshold;
		
		for(String cond:ConditionV2.allConditonV2s){
			YAxisMetric yMetric=qualityMetrics.get(cond);
			marketString+=","+cond;
			if(yMetric!=null) marketString+=","+String.format("%.3f", yMetric.qualityScore);
			else marketString+=",-1";
		}
		marketString+=","+tmcs.size();		
		marketString+=","+densityMetrics.sumOfProbes
					+","+densityMetrics.sumOfVehicles
					+","+String.format("%.2f", miles);
		
		return marketString;
	}
}
