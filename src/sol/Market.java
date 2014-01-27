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

class Conditon{
	public String timePeriod;
	public String engine;
	public String roadCondition;
	
	
	public Conditon(String engine){
		this.engine=engine;
	}
	public String toString(){
		return engine+" "+timePeriod+" "+roadCondition;
	}
	public boolean equals(Object other){
		return engine.equals(((Conditon)other).engine);
	}
}


public class Market {
	public String name;
	public HashSet<String> tmcs;
	
	//public Conditon conditon;
	public XAXisMetric densityMetrics;
	public HashMap<String, YAxisMetric> qualityMetrics;
	
	public Market(String market){
		name=market;
		tmcs=new HashSet<String>();
		qualityMetrics=new HashMap<String, YAxisMetric>();
		qualityMetrics.put("HALO", new YAxisMetric(0));
		qualityMetrics.put("HTTM", new YAxisMetric(0));
		densityMetrics=new XAXisMetric();
	}
	
	public String toString(){
		String marketString=name;
		marketString+=","+String.format("%.3f", densityMetrics.avgProbeCntPerTMC)+","+String.format("%.3f", densityMetrics.avgVehicleCntPerTMC);
		String[] engines={"HALO","HTTM"};
		for(String engine:engines){
			YAxisMetric yMetric=qualityMetrics.get(engine);
			marketString+=","+String.format("%.3f", yMetric.qualityScore);
		}
		marketString+=","+tmcs.size();		
		marketString+=","+densityMetrics.sumOfProbes+","+densityMetrics.sumOfVehicles;
		
		return marketString;
	}
}
