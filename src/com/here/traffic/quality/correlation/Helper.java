package com.here.traffic.quality.correlation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.Variance;


class EpochTMCPairWithProbe{
	public static int EPOCH_SIZE_IN_SEC=180;
	String date;
	String country;
	String tmc;
	String endTimeGMC;
	int epoch;
	ArrayList<Probe> probes;
	double groundtruthSpd;
	double mlSpd;
	double error;
	double density;
	
	String id;
	public EpochTMCPairWithProbe(String id, String date, String country, String tmc, String endTimeGMC){
		this.id=id;
		this.date=date;
		this.country=country;
		this.tmc=tmc;
		this.endTimeGMC=endTimeGMC;
		this.epoch=CommonUtils.HMSToSeconds(endTimeGMC)/EPOCH_SIZE_IN_SEC;
		//this.error=Double.parseDouble(error);
		probes=new ArrayList<Probe>();
	}
	
	public EpochTMCPairWithProbe(String id, String date, String country, String tmc, String endTimeGMC, String density){
		this(id, date, country, tmc, endTimeGMC);
		this.density=Double.parseDouble(density);
	}
	
	public String toString(){
		String ret=tmc+","+endTimeGMC+","+String.format("%.2f", groundtruthSpd)+","+String.format("%.2f", error)+","+String.format("%.2f", density);
		Collections.sort(probes);
		if(probes.size()>0){
			ret+="\n";
			for(Probe probe: probes) ret+=probe.toString()+" , ";
			ret+="\n spdVar="+String.format("%.2f", avgAndStd[1]  );
		}
		return ret;
	}
	
	public double[] getProbeSpeed(){
		double[] spds=new double[probes.size()];
		for(int i=0;i<probes.size();i++) spds[i]=probes.get(i).speed;
		return spds;
	}
	
	
	double[] avgAndStd;
	public double[] getAvgStdProbeSpeed(){
		Variance var=new Variance();
		Mean mean=new Mean();
		double[] avgAndStd=new double[2];
		double[] values=getProbeSpeed();
		avgAndStd[0]=mean.evaluate(values);
		avgAndStd[1]=Math.sqrt(var.evaluate(values, avgAndStd[0]));
		return avgAndStd;
	}
}

class Probe implements Comparable<Probe>{
	String timestamp;
	double speed;
	String provider;
	public Probe(String timestamp, double speed, String provider) {
		this.timestamp=timestamp;
		this.speed=speed;
		this.provider=provider;
	}
	public String toString(){
		return timestamp+"+"+String.format("%.2f", speed);
	}
	public int compareTo(Probe other){
		return timestamp.compareTo(other.timestamp);
	}
}


public class Helper {
	public static void main(String[] args){
		analyze();
		//examine();
	}
	
	/**
	 * output some pairs with large density and small density
	 */
	public static void analyze(){
		String date="2013-12-12";
		String country="US";
		String engineType="HTTM";
		String resVersionString="v5";
		
		int noOfPairs=20;
		String[][] epochTMCPair=new String[noOfPairs][3];
		
		String filepath=Constants.RESULT_DATA+"v5/v5_"+date.replaceAll("-", "")+"_"+country+".csv";
		Scanner sc;
		try {
			sc = new Scanner(new File(filepath));
			int i=0;
//			String[] pair;
			while(sc.hasNextLine()){
				String line=sc.nextLine();
				String[] fields=line.split(",");
				
				double noOfProbesPerMile=Double.parseDouble(fields[5]);
				double groundTruthSpeed=Double.parseDouble(fields[fields.length-2]);
				double error=Math.abs(Double.parseDouble(fields[fields.length-1]));
							
				if(groundTruthSpeed>60){//either free or congestion
	//				pair=new String[2];
					if(
						(noOfProbesPerMile<=20&&noOfProbesPerMile>5&&error<2&&i<noOfPairs/2)
						|| (noOfProbesPerMile>=50&&error>10&&i>=noOfPairs/2)
					){
						epochTMCPair[i][0]=fields[2];
						epochTMCPair[i][1]=CommonUtils.secondsToHMS(Integer.parseInt(fields[1])*EpochTMCPairWithProbe.EPOCH_SIZE_IN_SEC);
						epochTMCPair[i][2]=String.valueOf(noOfProbesPerMile);
						i+=1;
					}
					//epochTMCPair[i++]=pair;
				}
				if(i==noOfPairs) break;
			}
			
			examine(date, country, engineType, epochTMCPair);
		
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * pull out the probe data for a EpochTMC pair
	 * given pair, find out the corresponding probe data
	 */
	public static void examine(){
		String date="2013-12-12";
		String country="US";
		String engineType="HTTM";
		String[][] epochTMCPair={
				{"107N05438", "17:21:46"},
				//{"118P06058", "14:19:33"}
		};
		examine(date, country, engineType, epochTMCPair);
	}

	public static void examine(String date, String country, String engineType, String[][] epochTMCPair){
		for(String[] pair:epochTMCPair)
			System.out.println(Arrays.toString(pair));
		
		HashMap<String, EpochTMCPairWithProbe> pairsWithProbe=new HashMap<String, EpochTMCPairWithProbe>();
		for(String[] pair: epochTMCPair){
			String id=pair[0]+","+date+" "+pair[1];
			EpochTMCPairWithProbe epochTMC=new EpochTMCPairWithProbe(id, date, country, pair[0], pair[1], pair[2]);
			pairsWithProbe.put(epochTMC.tmc+"-"+epochTMC.epoch, epochTMC);
		}
		
		
		try {
			//retrieve the groundtruth 
			String filePath=Constants.GROUND_TRUTH_DATA+country+"/groundtruth_"+date.replaceAll("-", "")+"_"+country+".txt";
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String line;
			int pairCnt=0;
			while((line=br.readLine())!=null){
				pairCnt+=1;
				String[] fields=line.split(",");
				String tmc=fields[10];			
				String endTimeGMT=fields[14];
				String engine=fields[11];
				String isHyw=fields[12];
				String flowType=fields[35];//36
				
				int epoch=CommonUtils.HMSToSeconds(endTimeGMT.split(" ")[1])/EpochTMCPairWithProbe.EPOCH_SIZE_IN_SEC;
				String key=tmc+"-"+epoch;
			
				if(isHyw.equals("Y")&&engine.equals(engineType)&&pairsWithProbe.containsKey(key)){
					double error=Double.parseDouble(fields[30]);//capped difference
					Double groudTruthSpeed=Double.parseDouble(fields[27]);
					EpochTMCPairWithProbe pair=pairsWithProbe.get(key);
					pair.groundtruthSpd=groudTruthSpeed;
					pair.mlSpd=groudTruthSpeed-error;
					pair.error=error;
					if(pairCnt==pairsWithProbe.size()){
						//System.out.println("here"+pair);
						break;
					}
					
				}
				
			}
			//print pairs without probes
			for(EpochTMCPairWithProbe pair: pairsWithProbe.values()){
				System.out.println(pair);
			}
			
			//retrieve raw probe data
			//String: tableID; HashSet<String> pairsInTheTable
			HashMap<String, HashSet<String>> tableIDs=new HashMap<String, HashSet<String>>();
			for(EpochTMCPairWithProbe pair: pairsWithProbe.values()){
				String tableId=pair.tmc.substring(1,3);
				if(!tableIDs.containsKey(tableId)){
					tableIDs.put(tableId,new HashSet<String>());
				}
				tableIDs.get(tableId).add(pair.tmc+"-"+pair.epoch);
			}
			System.out.println(tableIDs);
			
			for(String tableId: tableIDs.keySet()){
				int tableSeq=Integer.parseInt(tableId)/3*3;
				String tableSeqStr="";
				for(int i=1;i<=3;i++){
					tableSeqStr+=(tableSeq+i);
					if(i<3) tableSeqStr+=",";
				}
				
				String dt=date.replaceAll("-","");
				String rawProbeFilePath=Constants.PROBE_RAW_DATA+country+"/"+dt+"/"+dt+"_"+tableSeqStr+"_probe.csv";
				System.out.println(rawProbeFilePath);
				br = new BufferedReader(new FileReader(rawProbeFilePath));
				while((line=br.readLine())!=null){
					String[] fields=line.split(",");
					if(fields.length!=Constants.RAW_PROBE_IDX_TMC_POINT_LOC_CODE+1) continue;
					//throw away low speed data
					double speed=Double.parseDouble(fields[Constants.RAW_PROBE_IDX_SPEED]);
					if(speed<0.01){
						continue;
					}
					
					//build up tmc
					String tmc=fields[Constants.RAW_PROBE_IDX_CTY_CODE]+fields[Constants.RAW_PROBE_IDX_TABLE_ID];
					if(fields[Constants.RAW_PROBE_IDX_TMC_DIR].equals("+")||
							fields[Constants.RAW_PROBE_IDX_TMC_DIR].equals("P")	
							) tmc+="P";
					else{
						if(fields[Constants.RAW_PROBE_IDX_TMC_DIR].equals("-")||
								fields[Constants.RAW_PROBE_IDX_TMC_DIR].equals("N")	
								)
						tmc+="N";
						else {
							continue;
						}
					}
					tmc+=fields[Constants.RAW_PROBE_IDX_TMC_POINT_LOC_CODE];
					//System.out.println(tmc+" "+line);
									
					DateFormat simpDateFormat=new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");//must be small letters
					Date systeTimestamp=simpDateFormat.parse(
							//fields[Constants.RAW_PROBE_IDX_SYS_DATE]
							fields[Constants.RAW_PROBE_IDX_SAMPLE_DATE]
									);
					int delay=5;//minutes
					int lookback=10; //minutes
					int secondsOfDay=systeTimestamp.getHours()*3600+systeTimestamp.getMinutes()*60+systeTimestamp.getSeconds();
					int startEpochIdx=(secondsOfDay+delay*60)/180;
					int endEpochIdx=(secondsOfDay+(delay+lookback)*60)/180;
					
					
					for(int epochIdx=startEpochIdx;epochIdx<endEpochIdx;epochIdx++){
						String id=tmc+"-"+epochIdx;
						if(tmc.equals("107N05438")){
							//&&fields[Constants.RAW_PROBE_IDX_SYS_DATE].contains("17:") ){
							//System.out.println(id);
						}
						
						EpochTMCPairWithProbe epochTMC;
						//a new tmc-epoch pair
						String table=tmc.substring(1, 3);
						if(!tableIDs.containsKey(table)||!tableIDs.get(table).contains(id)){
							continue;
						}else{
							epochTMC=pairsWithProbe.get(id);
						}
						epochTMC.probes.add(new Probe(CommonUtils.secondsToHMS(secondsOfDay), speed, fields[Constants.RAW_PROBE_IDX_VEHICLE_ID]));
					}
				}
			}
			
			//calcule the avg std of spd for different groups
			double[] avgSqrt=new double[2];
			//print out pairs with probes
			for(EpochTMCPairWithProbe pair: pairsWithProbe.values()){
				pair.avgAndStd=pair.getAvgStdProbeSpeed();
				System.out.println(pair);
				if(pair.density>=50) avgSqrt[1]+=pair.avgAndStd[1];
				else avgSqrt[0]+=pair.avgAndStd[1];
			}
			for(int z=0;z<avgSqrt.length;z++) avgSqrt[z]/=epochTMCPair.length/2;
			System.out.println(Arrays.toString(avgSqrt));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
