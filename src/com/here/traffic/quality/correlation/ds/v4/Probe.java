package com.here.traffic.quality.correlation.ds.v4;

import com.here.traffic.quality.correlation.CommonUtils;


public class Probe implements Comparable<Probe>{
	public static enum SOURCE{
		QML, ARCHIVE;
	}
	
	public String timestamp;
	public double speed;
	public String provider;
	public int secondOfDay;
	public String id;
	public String vehicle;
	public Probe(String timestamp, double speed, String provider, String vehicleID) {
		this.timestamp=timestamp; //in the form of yyyy-MM-dd hh:mm:ss
		this.secondOfDay=CommonUtils.HMSToSeconds(timestamp.split(" ")[1]);
		this.speed=speed;
		this.provider=provider;
		this.vehicle=vehicleID;
		this.id=timestamp+"-"+provider+"-"+vehicleID;
	}
	
	
	public String toString(){
		return timestamp+"-"+String.format("%.2f", speed);
	}
	public int compareTo(Probe other){
		return timestamp.compareTo(other.timestamp);
	}
}
