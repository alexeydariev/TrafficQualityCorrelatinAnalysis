package com.here.traffic.quality.correlation.ds;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import com.here.traffic.quality.correlation.Constants;

public class TMC {
	public String tmc;
	public double miles;
	public boolean minAccessControl;
	public boolean maxAccessControl;
	public String extendedCountryCode;
	
	
	//lane related
	//public int minNoOfLane;
	public int maxNoOfLane;
	public boolean maxExpressLane;
	public boolean maxCarpoolLane;
	
	
	public TMC(String tmc, double miles, boolean minAC, boolean maxAC, String cc){
		this.tmc=tmc;
		this.miles=miles;
		minAccessControl=minAC;
		maxAccessControl=maxAC;
		extendedCountryCode=cc;
	}
	
	public static HashMap<String, TMC> loadTMC(String country){
		HashMap<String, TMC> tmcs=new HashMap<String, TMC>();
		try{
			BufferedReader br;
			FileWriter fw=null;
			File tmcTable=new File(Constants.TMC_DATA+"tmc_"+country+".txt");
			if(tmcTable.exists()){
				br = new BufferedReader(new FileReader(tmcTable));
			}else{
				br = new BufferedReader(new FileReader(Constants.TMC_DATA+"tmc_attribute.txt"));
				fw=new FileWriter(tmcTable);
				System.out.println("fw is not null.");
			}
			
			switch (country) {
			case "US":
				country="USA";
				break;
			case "France":
				country="FRA";
			default:
				break;
			}
			
			String line;
			while ((line = br.readLine()) != null) {
				//System.out.println(line);
				String[] fields=line.split(",");
				if(!fields[69].equals(country)) continue;
				if(fw!=null){
					fw.write(line+"\n");
					//System.out.println(line);
				}
				String tmcID=fields[0];
				double miles=Double.parseDouble(fields[1]);
				boolean minAC=fields[42].equals("Y")?true:false,maxAC=fields[68].equals("Y")?true:false;
				TMC tmc=new TMC(tmcID, miles, minAC, maxAC, country);
				tmc.maxNoOfLane=Math.max(Integer.parseInt(fields[56]), Integer.parseInt(fields[57]));
				tmc.maxExpressLane=Boolean.parseBoolean(fields[86]);
				tmc.maxCarpoolLane=Boolean.parseBoolean(fields[87])|Boolean.parseBoolean(fields[88]);
				
				tmcs.put(tmcID, tmc);
			}
			br.close();
			if(fw!=null) fw.close();
			System.out.println(tmcs.size()+" TMC's are loaded from the TMC table.");
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return tmcs;
	}
	
}
