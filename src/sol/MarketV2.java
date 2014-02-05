package sol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;




class XAXisMetric{
	public double avgProbeCntPerTMC;
	public double avgVehicleCntPerTMC;
	public double avgProviderCntPerTMC;
	
	public double sumOfProbes;
	public double sumOfVehicles;
	public double sumOfProviders;
	
	public double noOfTMCsWithProbeCntOverThreshold;
	public double noOfTMCsWithVehicleCntOverThreshold;
}


class YAxisMetric{
	public double qualityScore; //just for one day
	
	public YAxisMetric(double qs){
		qualityScore=qs;
	}
}

class ConditonV2{
	
	public String timePeriod;
	public String engine;
	public String roadCondition;
	
	public static HashSet<String> allConditonV2s=new HashSet<String>();
	
	public ConditonV2(String engine, String timePeriod, String roadCondition){
		this.engine=engine;
		this.timePeriod=timePeriod;
		this.roadCondition=roadCondition;
	}
	public String toString(){
		return engine+"-"+timePeriod+"-"+roadCondition;
	}

}


public class MarketV2 {
	public String name;
	
	public String date;
	public HashSet<String> tmcs;
	public double miles;
	
	
	//public Conditon conditon;
	public XAXisMetric densityMetrics;
	public HashMap<String, YAxisMetric> qualityMetrics;
	
	public MarketV2(String market, String date){
		name=market;
		this.date=date;
		tmcs=new HashSet<String>();
		qualityMetrics=new HashMap<String, YAxisMetric>();
		densityMetrics=new XAXisMetric();
	}
	
	public MarketV2(String[] fields){
		name=fields[0];
		date=fields[1];
		densityMetrics=new XAXisMetric();
			densityMetrics.avgProbeCntPerTMC=Double.parseDouble(fields[2]);
			densityMetrics.avgVehicleCntPerTMC=Double.parseDouble(fields[3]);
			densityMetrics.noOfTMCsWithProbeCntOverThreshold=Double.parseDouble(fields[4]);
			densityMetrics.noOfTMCsWithVehicleCntOverThreshold=Double.parseDouble(fields[5]);
		qualityMetrics=new HashMap<String, YAxisMetric>();
		int idx=6;
		while(idx<fields.length-4){
			qualityMetrics.put(fields[idx], new YAxisMetric(Double.parseDouble(fields[idx+1])));
			ConditonV2.allConditonV2s.add(fields[idx]);
			
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
		for(String cond: ConditonV2.allConditonV2s){
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
		
		for(String cond:ConditonV2.allConditonV2s){
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
