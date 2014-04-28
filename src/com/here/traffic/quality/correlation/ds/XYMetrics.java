package com.here.traffic.quality.correlation.ds;

import java.util.ArrayList;

public class XYMetrics {
	public double avgCoverage;
	public double avgRMSE;
	public double avgTmcCnt;
	public ArrayList<Double> coverages; //each value is the coverage for a epoch
	public ArrayList<Double> rootMeanSquareErrors; //each value is the error for each epoch
	public ArrayList<Double> tmcCnts; //each value is the # of tmcs for each epoch
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
