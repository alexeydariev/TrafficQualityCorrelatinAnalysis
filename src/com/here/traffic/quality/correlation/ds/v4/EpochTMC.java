package com.here.traffic.quality.correlation.ds.v4;

import java.util.ArrayList;
import java.util.HashSet;

public class EpochTMC {
	public static int ATOMIC_EPOCH_DURATION=3; //in minutes
	public int epochIdx; //index of epoch of a day, e.g. 1 means the 1st epoch of the day
	public String tmc; //tmc code
	public String date;
	
	
	public boolean covered; //true if have real-time data
	
	public double noOfProbes;
	public HashSet<String> vehicleSet;
	public int noOfVehicles;
	
	public HashSet<String> providerSet;
	public int providerSetSize;
	
	public int noOfProviders;
	public double noOfProbesPerMile;
	
	public ArrayList<Probe> probes;//store the speed of probes corresponding to this pair
	public HashSet<String> probeIDs; //timestamp+provideID+vehicleID
	
	public double probeSpeedMean;
	public double probeSpeedStd;
	
	
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
		probes=new ArrayList<Probe>();
		probeIDs=new HashSet<String>();
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
				
				+","+String.format("%.2f", probeSpeedMean)+","+String.format("%.2f", probeSpeedStd)
				
				+","+vehicleSet.size()+","+providerSet.size()
				+","+String.format("%.2f", groundTruthSpeed)+","+String.format("%.2f", error);
	}
	
}
