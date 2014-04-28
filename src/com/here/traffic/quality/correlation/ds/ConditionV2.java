package com.here.traffic.quality.correlation.ds;

import java.util.HashSet;

public class ConditionV2 {
	public String timePeriod;
	public String engine;
	public String roadCondition;
	
	public static HashSet<String> allConditonV2s=new HashSet<String>();
	
	public ConditionV2(String engine, String timePeriod, String roadCondition){
		this.engine=engine;
		this.timePeriod=timePeriod;
		this.roadCondition=roadCondition;
	}
	public String toString(){
		return engine+"-"+timePeriod+"-"+roadCondition;
	}
}
