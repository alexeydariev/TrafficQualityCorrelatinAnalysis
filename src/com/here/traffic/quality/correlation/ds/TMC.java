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
	
	public int minNoOfLane;
	public int maxNoOfLane;
	
	public TMC(String tmc, double miles, boolean minAC, boolean maxAC, String cc){
		this.tmc=tmc;
		this.miles=miles;
		minAccessControl=minAC;
		maxAccessControl=maxAC;
		extendedCountryCode=cc;
	}
	
	public static HashMap<String, TMC> loadTMC(String country){
		return loadTMC(country,false);
	}
	
	public static HashMap<String, TMC> loadTMC(String country, boolean loadLane){
		HashMap<String, TMC> tmcs=new HashMap<String, TMC>();
		try{
			BufferedReader br;
			FileWriter fw=null;
			File areaTMCToMarket=new File(Constants.TMC_DATA+"tmc_"+country+".txt");
			if(areaTMCToMarket.exists()){
				br = new BufferedReader(new FileReader(areaTMCToMarket));
			}else{
				br = new BufferedReader(new FileReader(Constants.TMC_DATA+"tmc_attribute.txt"));
				fw=new FileWriter(areaTMCToMarket);
				System.out.println("fw is not null.");
			}
			
			String line;
			while ((line = br.readLine()) != null) {
				String[] fields=line.split(",");
				if(!fields[69].equals(country)) continue;
				if(fw!=null){
					fw.write(line+"\n");
					//System.out.println(line);
				}
				String tmc=fields[0];
				double miles=Double.parseDouble(fields[1]);
				boolean minAC=fields[42].equals("Y")?true:false,maxAC=fields[68].equals("Y")?true:false;
				tmcs.put(tmc, new TMC(tmc, miles, minAC, maxAC, country));
			}
			br.close();
			if(fw!=null) fw.close();
			
			
			//
			if(loadLane){
				Scanner sc=new Scanner(new File(Constants.TMC_DATA+"\tmc_lane_"+country+".txt"));
				while(sc.hasNextLine()){
					line=sc.nextLine();
					String[] fields=line.split(",");
					if(!tmcs.containsKey(fields[0])) continue; //not continues;
					TMC tmc=tmcs.get(fields[0]);
					if(fields[0].contains("P")||fields[0].contains("+")){
						tmc.minNoOfLane=Integer.parseInt(fields[1]);
						tmc.maxNoOfLane=Integer.parseInt(fields[3]);
					}else{
						tmc.minNoOfLane=Integer.parseInt(fields[2]);
						tmc.maxNoOfLane=Integer.parseInt(fields[4]);
					}
				}
				sc.close();
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return tmcs;
	}
	
	
	public static HashMap<String, ArrayList<Integer>> loadTMCLane(String country){
		HashMap<String, ArrayList<Integer>> tmcLane=new HashMap<String, ArrayList<Integer>>();
		try{
			Scanner sc=new Scanner(new File(Constants.TMC_DATA+"\tmc_lane_"+country+".txt"));
			while(sc.hasNextLine()){
				String line=sc.nextLine();
				String[] fields=line.split(",");
				ArrayList<Integer> lanes=new ArrayList<Integer>();
				if(fields[0].contains("P")||fields[0].contains("+")){
					lanes.add(Integer.parseInt(fields[1]));
					lanes.add(Integer.parseInt(fields[3]));
				}else{
					lanes.add(Integer.parseInt(fields[2]));
					lanes.add(Integer.parseInt(fields[4]));
				}
				tmcLane.put(fields[0], lanes);
			}
			sc.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return tmcLane;
	}
	
	
	
}
