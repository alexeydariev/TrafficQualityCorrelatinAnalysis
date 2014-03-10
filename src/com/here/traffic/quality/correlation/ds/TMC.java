package com.here.traffic.quality.correlation.ds;

public class TMC {
	public String tmc;
	public double miles;
	public boolean minAccessControl;
	public boolean maxAccessControl;
	public String extendedCountryCode;
	
	public TMC(String tmc, double miles, boolean minAC, boolean maxAC, String cc){
		this.tmc=tmc;
		this.miles=miles;
		minAccessControl=minAC;
		maxAccessControl=maxAC;
		extendedCountryCode=cc;
	}
	
}
