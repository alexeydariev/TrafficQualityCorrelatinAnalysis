package sol;

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

class XYMetrics{
	public double avgCoverage;
	public double avgRMSE;
	public double avgTmcCnt;
	ArrayList<Double> coverages; //each value is the coverage for a epoch
	ArrayList<Double> rootMeanSquareErrors; //each value is the error for each epoch
	ArrayList<Double> tmcCnts; //each value is the # of tmcs for each epoch
	/**
	 * variables for stats in an epoch
	 */
		public int noOfTMCsInOneEpoch; 
		public int noOfCoveredTMCsInOneEpoch;
		public double squareErrorInOneEpoch;
	
	public XYMetrics(){
		coverages=new ArrayList<Double>();
		rootMeanSquareErrors=new ArrayList<Double>();
		tmcCnts=new ArrayList<Double>();
	}
		
	public static String getHeader(){
		return "avgCoverage,avgRMSE,avgTmcCnt,noOfEpoches";
	}
		
	public String toString(){
		return String.format("%.2f",avgCoverage)+","+String.format("%.2f", avgRMSE)
			+","+String.format("%.2f", avgTmcCnt)+","+coverages.size();
		//return noOfCoveredTMCsInOneEpoch+","+noOfTMCsInOneEpoch+","+String.format("%.2f", errorInOneEpoch);
	}
}


class EpochTMC {
	public static int ATOMIC_EPOCH_DURATION=3; //in minutes
	public int epochIdx; //index of epoch of a day, e.g. 1 means the 1st epoch of the day
	public String tmc; //tmc code
	public String date;
	
	
	public boolean covered; //true if have real-time data
	
	public double noOfProbes;
	public HashSet<String> vehicleSet;
	public int noOfVehicles;
	public HashSet<String> providerSet;
	public int noOfProviders;
	public double noOfProbesPerMile;
	
	
	public double fieldToBeSorted;
	
	public double groundTruthSpeed;
	public double error;//diff between the GT and the predicated speed	
	public String condition;
	
	public EpochTMC(String date, String tmc, int epochIdx){
		this.date=date;
		this.tmc=tmc;
		this.epochIdx=epochIdx;
		vehicleSet=new HashSet<String>();
		providerSet=new HashSet<String>();		
	}
	
	public EpochTMC(String date, String tmc, int epochIdx, double error, String condition, double groundTruthSpeed){
		this(date, tmc, epochIdx);
		this.error=error;
		this.condition=condition;
		this.groundTruthSpeed=groundTruthSpeed;
	}
	
	public EpochTMC(String date, String tmc, int epochIdx, boolean covered, double error, String condition, double groundTruthSpeed){
		this(date, tmc, epochIdx,error,condition, groundTruthSpeed);
		this.covered=covered;
	}
	
	public void setToBeSortedField(double value){
		fieldToBeSorted=value;
	}
	
	
	public String getID(){
		return date+"-"+epochIdx+"-"+tmc;
	}
	
	//serialize
	public String toString(){
		return date+","+epochIdx+","+tmc+","+condition
				+","+(int)noOfProbes+","+String.format("%.2f", noOfProbesPerMile)
				+","+vehicleSet.size()+","+providerSet.size()
				+","+String.format("%.2f", groundTruthSpeed)+","+error;
	}
	
	
}


