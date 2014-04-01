package com.here.traffic.quality.correlation.ds.v4;

import com.here.traffic.quality.correlation.CommonUtils;


public class Probe implements Comparable<Probe>{
	public String timestamp;
	public double speed;
	public String provider;
	public int secondOfDay;
	public Probe(String timestamp, double speed, String provider) {
		this.timestamp=timestamp;
		this.secondOfDay=CommonUtils.HMSToSeconds(timestamp.split(" ")[1]);
		this.speed=speed;
		this.provider=provider;
	}
	public String toString(){
		return timestamp+"-"+String.format("%.2f", speed);
	}
	public int compareTo(Probe other){
		return timestamp.compareTo(other.timestamp);
	}
}
