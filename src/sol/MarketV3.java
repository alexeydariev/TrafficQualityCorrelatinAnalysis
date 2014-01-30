package sol;

import java.util.ArrayList;
import java.util.HashMap;

public class MarketV3 {
	public String marketName;
	
	public HashMap<String, XYMetrics> conditionResults;
	
	public MarketV3(String marketName){
		this.marketName=marketName;
		conditionResults=new HashMap<String, XYMetrics>();
	}
	
	public String toString(){
		String ret=marketName+"\t";
		for(String condition: conditionResults.keySet()){
			ret+=condition+":"+conditionResults.get(condition)+"\t";
		}
		return ret;
	}
}

class XYMetrics{
	public double avgCoverage;
	public double avgError;
	ArrayList<Double> coverages; //each value is the coverage for a epoch
	ArrayList<Double> errors; //each value is the error for each epoch
	/**
	 * variables for stats in an epoch
	 */
		public int noOfTMCsInOneEpoch; 
		public int noOfCoveredTMCsInOneEpoch;
		public double errorInOneEpoch;
	
	public XYMetrics(){
		coverages=new ArrayList<Double>();
		errors=new ArrayList<Double>();
	}
		
	public String toString(){
		return String.format("%.2f",avgCoverage)+","+String.format("%.2f", avgError)+","+coverages.size();
		//return noOfCoveredTMCsInOneEpoch+","+noOfTMCsInOneEpoch+","+String.format("%.2f", errorInOneEpoch);
	}
}


class EpochTMC {
	public static int EPOCH_DURATION=3; //in minutes
	
	public int epochIdx; //index of epoch of a day, e.g. 1 means the 1st epoch of the day
	public String tmc; //tmc code
	
	public boolean covered; //true if have real-time data
	public int noOfProbes;
	
	public double error;//diff between the GT and the predicated speed
	
	public String condition;
	
	public EpochTMC(String tmc, int epochIdx, boolean covered, double error, String condition){
		this.tmc=tmc;
		this.epochIdx=epochIdx;
		this.covered=covered;
		this.error=error;
		this.condition=condition;
	}
}


