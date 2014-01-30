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
	
	
	public ConditonV2(String engine, String timePeriod, String roadCondition){
		this.engine=engine;
		this.timePeriod=timePeriod;
		this.roadCondition=roadCondition;
	}
	public String toString(){
		return engine+" "+timePeriod+" "+roadCondition;
	}
	
}


public class MarketV2 {
	public String name;
	public HashSet<String> tmcs;
	
	//public Conditon conditon;
	public XAXisMetric densityMetrics;
	public HashMap<ConditonV2, YAxisMetric> qualityMetrics;
	
	public MarketV2(String market){
		name=market;
		tmcs=new HashSet<String>();
		qualityMetrics=new HashMap<ConditonV2, YAxisMetric>();
		densityMetrics=new XAXisMetric();
	}
	
	public String toString(){
		String marketString=name;
		marketString+=","+String.format("%.3f", densityMetrics.avgProbeCntPerTMC)+","+String.format("%.3f", densityMetrics.avgVehicleCntPerTMC)
				+","+densityMetrics.noOfTMCsWithProbeCntOverThreshold+","+densityMetrics.noOfTMCsWithVehicleCntOverThreshold;
		
		for(ConditonV2 condition:qualityMetrics.keySet()){
			YAxisMetric yMetric=qualityMetrics.get(condition);
			marketString+=","+String.format("%.3f", yMetric.qualityScore);
		}
		marketString+=","+tmcs.size();		
		marketString+=","+densityMetrics.sumOfProbes+","+densityMetrics.sumOfVehicles;
		
		return marketString;
	}
}
