package sol;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import javax.swing.text.AbstractDocument.LeafElement;

import org.apache.commons.math3.analysis.function.Exp;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.descriptive.moment.Mean;

import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;


public class CorrelationAnalysis {
	public static void main(String[] args) {
		new CorrelationAnalysis().main();
	}
	
	public static String analysisVersion;
	
	public void main(){
//TODO main
		//secondAttempt();
		//thirdAttempt();
		
		//addProbeCntToMarketv3();
		
		//v4OutputStatResults();
		v4Attempt();
		
	}
	
	
	public void addProbeCntToMarketv3(){
		HashMap<String, MarketV2> marketV2s=loadMarketV2s();
		for(MarketV3 market: loadMarketV3s().values()){
			System.out.print(market.toString()+",");
			if(marketV2s.containsKey(market.marketName)){
				MarketV2 m2=marketV2s.get(market.marketName);
				System.out.println(
						String.format("%.2f", m2.densityMetrics.sumOfProbes/m2.miles)
						+","+String.format("%.2f", m2.densityMetrics.sumOfVehicles/m2.miles)
				);
			}else{
				System.out.println("-1,-1");
			}
		}
	}
	
	
	public void v4OutputStatResults(){
		analysisVersion="v4_";	
		String[] dates={"20131213"};
		HashMap<String, EpochTMC> epochTMCPairs;
		ArrayList<EpochTMC> epochTMCPairsWithGroundTruh=new ArrayList<EpochTMC>();
		
		FileWriter fw;
		
		try{
			//produce stat files
			for(String date: dates){
				for(int batch=0;batch<10;batch++){
					String batchString=(3*batch+1)+","+(3*batch+2)+","+(3*batch+3);
					epochTMCPairs=readRawProbeFileOutputDensityByTMC(date+"_"+batchString+"_probe.csv",date , 180);
					
					//update the error of epochTMC pairs
					BufferedReader br = new BufferedReader(new FileReader(Constants.GROUND_TRUTH_DATA+"ground_truth_"+date+".txt"));
					String line;
					while((line=br.readLine())!=null){
						String[] fields=line.split(",");
						String tmc=fields[10];
						//get the epochIdx
						int epochIdx=Integer.parseInt(fields[8])/180; //each epoch is 3 minutes
						double error=Double.parseDouble(fields[30]);//capped difference
						String engine=fields[11];
						String isHyw=fields[12];
						String flowType=fields[36];
						
						String id=date+"-"+epochIdx+"-"+tmc;
						if(epochTMCPairs.containsKey(id)){
							EpochTMC epochTMC=epochTMCPairs.get(id);
							epochTMC.error=error;
							epochTMC.condition=engine+"-"+flowType+"-"+isHyw;
						}
					}
					br.close();
					
					//remove all pairs without ground truth
					for(EpochTMC epochTMC: epochTMCPairs.values()){
						if(epochTMC.error>0){//only keep epoch-tmc pairs with ground truth 
							epochTMCPairsWithGroundTruh.add(epochTMC);
						}
					}					
					
				}
				
				//output the stats to a file
				fw=new FileWriter(Constants.BIN_FOLDER+analysisVersion+date+".csv");
				for(EpochTMC epochTMC: epochTMCPairsWithGroundTruh){
					fw.write(epochTMC+"\n");//only write epoch-tmc pairs with ground truth 
				}
				fw.close();
			}	
			
			
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public ArrayList<EpochTMC> v4ReadStatResult(String date, String engine, boolean congestion){
		ArrayList<EpochTMC> epochTMCs=new ArrayList<EpochTMC>();
		//read stat file and analyze
		try{
			Scanner sc=new Scanner(new File(Constants.BIN_FOLDER+"v4_"+date+".csv"));
			while(sc.hasNextLine()){
				String line=sc.nextLine();
				String[] fields=line.split(",");
				EpochTMC epochTMC=new EpochTMC(fields[0], fields[2], Integer.parseInt(fields[1]));
				epochTMC.condition=fields[3];
				if(!epochTMC.condition.startsWith(engine)||
				!(epochTMC.condition.contains("Free")^congestion) ) continue;
				epochTMC.noOfProbes=Integer.parseInt(fields[4]);
				epochTMC.error=Double.parseDouble(fields[fields.length-1]);
				epochTMCs.add(epochTMC);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return epochTMCs;
	}
	
	public void v4Attempt(){
		//v4OutputStatResults();
		String engine="HTTM";
		boolean[] isCongestions={false, true};
		String date="20131212";
		
		for(boolean isCongestion: isCongestions){
			ArrayList<EpochTMC> epochTMCs=v4ReadStatResult(date,engine, isCongestion);
			//sort epochTMC based on # of probes
			Collections.sort(epochTMCs, new Comparator() 
			{
			    public int compare(Object o1, Object o2) 
			    {
			       if(o1 instanceof EpochTMC && o2 instanceof EpochTMC) 
			       {
			         return ((EpochTMC)o1).noOfProbes-((EpochTMC)o2).noOfProbes;
			       } 
			       return 0;    
			    }
			});
			//TODO parameters		
			int INI_SIZE=1000;
			int MIN_NO_SAMPLES_IN_A_BIN=100;
			double[] binsOfDensity=new double[INI_SIZE];//of probes;
			
			int noOfBins=1000;int binStep=2;
			double[] hardCodedBins=new double[noOfBins];
			for(int i=1;i<noOfBins;i++) hardCodedBins[i]=hardCodedBins[i-1]+binStep;
			
			//System.out.println(Arrays.toString(binsOfDensity));
						
		
			double[] avgDensity=new double[INI_SIZE];
			double[] avgQualityScore=new double[INI_SIZE];
			double[] avgErrors=new double[INI_SIZE];
			int[] cntOfDensity=new int[INI_SIZE];
			int[] cntOfErrorAboveTreshold=new int[INI_SIZE];
			int binIdx=1;	
			
			int nextHardCodedBinIdx=1;
			for(EpochTMC epochTMC: epochTMCs){
				if(binIdx<binsOfDensity.length&&cntOfDensity[binIdx-1]>MIN_NO_SAMPLES_IN_A_BIN
		&&nextHardCodedBinIdx<hardCodedBins.length&&epochTMC.noOfProbes>hardCodedBins[nextHardCodedBinIdx]){
					binsOfDensity[binIdx]=epochTMC.noOfProbes;
					binIdx++;
					nextHardCodedBinIdx++;
				}
				
				cntOfDensity[binIdx-1]+=1;
				avgErrors[binIdx-1]+=epochTMC.error;
				avgDensity[binIdx-1]+=epochTMC.noOfProbes;
				if(epochTMC.error>10) cntOfErrorAboveTreshold[binIdx-1]++;
			}			
			//System.out.println("densityIdx="+binIdx);
			for(int i=0;i<binIdx;i++){
				avgErrors[i]/=cntOfDensity[i];
				avgDensity[i]/=cntOfDensity[i];
				avgQualityScore[i]=((int)((1-(cntOfErrorAboveTreshold[i]+0.0)/cntOfDensity[i])*10000))/100.0;
			}
			
			//print out Title of the stats
			String title="\t All Markets in US: HTTM-";
			if(isCongestion) title+="Congestion";
			else title+="Free Flow";
			title+=" on "+date;
			System.out.println(title);
			
			System.out.println("densityRange,#ofEpochTMCPairs,avgDensity,avgError,avgQS");
			for(int i=0;i<binIdx;i++){
				if(i>0) System.out.print("["+String.format("%.1f", binsOfDensity[i])+"~");
				else System.out.print("[ 1.0~");
				if(i<binIdx-1) System.out.print(String.format("%.1f",binsOfDensity[i+1])+"],");
				else System.out.print("     ],");
				System.out.print(cntOfDensity[i]+",");
				System.out.println(
						String.format("%.2f", avgDensity[i])
						+","+String.format("%.2f", avgErrors[i])
						+","+avgQualityScore[i]+"%"
				);
			}
			
			avgDensity=Arrays.copyOf(avgDensity, binIdx);
			avgErrors=Arrays.copyOf(avgErrors, binIdx);
			avgQualityScore=Arrays.copyOf(avgQualityScore, binIdx);
			PearsonsCorrelation pc=new PearsonsCorrelation();
			System.out.println("correlation between avgDensity and avgError is "+String.format("%.2f", pc.correlation(avgDensity, avgErrors)));
			System.out.println("correlation between avgDensity and avgQualityScore is "+String.format("%.2f", pc.correlation(avgDensity, avgQualityScore)));
			System.out.println();
			
			
		}
		
	}
	
	public void v3Attempt(){
		try{
			analysisVersion="v3_";			
			
			String[] dates={"20131212"};
			
			for(String date: dates){
				HashMap<Integer, ArrayList<EpochTMC>> epochTMCPairsIndexedByEpoch=new HashMap<Integer, ArrayList<EpochTMC>>();
				//build up epochs
				
				BufferedReader br = new BufferedReader(new FileReader(Constants.GROUND_TRUTH_DATA+"ground_truth_"+date+".txt"));
				String line;
				while((line=br.readLine())!=null){
					String[] fields=line.split(",");
					String tmc=fields[10];
					//get the epochIdx
					int epochIdx=Integer.parseInt(fields[8])/180; //each epoch is 3 minutes
					boolean covered; 
					if(Double.parseDouble(fields[25])>.85) covered=true;
					else covered=false;
					double error=Double.parseDouble(fields[30]);//capped difference
					String engine=fields[11];
					String isHyw=fields[12];
					String flowType=fields[36];
					
					EpochTMC epochTMC=new EpochTMC(date, tmc, epochIdx, covered, error, engine+"-"+flowType+"-"+isHyw);
					if(!epochTMCPairsIndexedByEpoch.containsKey(epochIdx)) epochTMCPairsIndexedByEpoch.put(epochIdx, new ArrayList<EpochTMC>());
					epochTMCPairsIndexedByEpoch.get(epochIdx).add(epochTMC);
				}
				System.out.println("# of epoches in the ground truth ="+epochTMCPairsIndexedByEpoch.size());
				//for(int epoch: epochTMCPairsIndexedByEpoch.keySet()) System.out.println(epochTMCPairsIndexedByEpoch.get(epoch).size()+"  ");
				br.close();
				
				/**
				 * Calculate the stats
				 */
				HashMap<String, String> tmcToMarket=loadTMCToMarket("A0", null, null); 
				HashMap<String, MarketV3> markets=new HashMap<String, MarketV3>();
				for(int epochIdx=0;epochIdx<24*60/EpochTMC.ATOMIC_EPOCH_DURATION;epochIdx++){
					if (epochTMCPairsIndexedByEpoch.containsKey(epochIdx)) {
						//for each epoch
						for(MarketV3 market: markets.values()){
							for(XYMetrics xyMetrics: market.conditionResults.values()){
								xyMetrics.noOfCoveredTMCsInOneEpoch=0;
								xyMetrics.noOfTMCsInOneEpoch=0;
								xyMetrics.squareErrorInOneEpoch=0;
							}
						}					
						for(EpochTMC pair: epochTMCPairsIndexedByEpoch.get(epochIdx)){
							String marketName=tmcToMarket.get(pair.tmc);
							if(!markets.containsKey(marketName)){
								markets.put(marketName, new MarketV3(marketName, date));
							}
							MarketV3 market= markets.get(marketName);
							if(!market.conditionResults.containsKey(pair.condition)){
								market.conditionResults.put(pair.condition, new XYMetrics());
							}
							MarketV3.allConditions.add(pair.condition);
							
							XYMetrics xyMetrics=market.conditionResults.get(pair.condition);
							xyMetrics.noOfTMCsInOneEpoch+=1;
							if(pair.covered) xyMetrics.noOfCoveredTMCsInOneEpoch+=1;
							xyMetrics.squareErrorInOneEpoch+=Math.pow(pair.error,2);
							
						}
						for(MarketV3 market: markets.values()){
							//System.out.println("# of conditions :"+market.conditionResults.size());
							for(XYMetrics xyMetrics: market.conditionResults.values()){
								if(xyMetrics.noOfTMCsInOneEpoch>0){
									xyMetrics.coverages.add((xyMetrics.noOfCoveredTMCsInOneEpoch+0.0)/xyMetrics.noOfTMCsInOneEpoch);
									double rmse=Math.sqrt(xyMetrics.squareErrorInOneEpoch/xyMetrics.noOfTMCsInOneEpoch);
									xyMetrics.rootMeanSquareErrors.add(rmse);
									xyMetrics.tmcCnts.add(xyMetrics.noOfTMCsInOneEpoch+0.0);
								}
							}
							//System.out.println(market);
						}
					}				
				}			
				
				//calculate the average of the epochs
				Mean mean=new Mean();
				for(MarketV3 market: markets.values()){
					for(XYMetrics xyMetrics: market.conditionResults.values()){
						xyMetrics.avgCoverage=mean.evaluate(CommonUtils.doubleListToDoubleArray(xyMetrics.coverages));
						xyMetrics.avgRMSE=mean.evaluate(CommonUtils.doubleListToDoubleArray(xyMetrics.rootMeanSquareErrors));
						xyMetrics.avgTmcCnt=mean.evaluate(CommonUtils.doubleListToDoubleArray(xyMetrics.tmcCnts));
					}
				}
				
				
				//Output to a file
				//metropolitans
				int cnt=0;
				FileWriter fw=new FileWriter(Constants.BIN_FOLDER+analysisVersion+date+".csv");
				fw.write(MarketV3.getHeader()+"\n");
				for(MarketV3 market: markets.values()){
					cnt++;
					fw.write(market+"\n");
				}
				//System.out.println("*********** "+cnt+" Metros ****************");
				fw.close();
				System.out.println("*********** "+cnt+" Cities ****************");
			}
			
			mergeResultFiles("v3");
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public void v2Attempt(){
		analysisVersion="v2_";

		HashMap<String, TMC> tmcs=loadTMC("A0");
		
		String[] dates={"20131213","20131212","20131220"};
		
		for(String date:dates){
			HashMap<String, MarketV2> markets=readGroundTruth(date);
			//for(Market market: markets.values()) System.out.println(market);		
			HashMap<String, String> tmcToMarket=loadTMCToMarket("A0", markets, tmcs); 

			
			File baseFolder=new File(Constants.PROBE_STAT_DATA+date+"/");
			File[] files=baseFolder.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					if(name.startsWith("STAT_")&&name.endsWith(".csv")){
						return true;
					}
					return false;
				}
			});
			

			for(File statFile: files){
				//String filepath=Constants.DATA_BASE_FOLDER+"STAT_20131212_6,7,8_probe.csv";
				v2ReadStatResults(tmcs,markets, tmcToMarket, statFile.getAbsolutePath());
			}
			try{
				FileWriter fw=new FileWriter(Constants.BIN_FOLDER+analysisVersion+date+".csv");
				fw.write(MarketV2.getHeader()+"\n");
				for(String name: markets.keySet()){
					MarketV2 market=markets.get(name);
					XAXisMetric metric=market.densityMetrics;
					metric.avgProbeCntPerTMC=metric.sumOfProbes/market.tmcs.size();
					metric.avgVehicleCntPerTMC=metric.sumOfVehicles/market.tmcs.size();
					fw.write(market+"\n");
				}
				fw.close();
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		mergeResultFiles("v2");
	}
	
	
	public HashMap<String, EpochTMC> readRawProbeFileOutputDensityByTMC(String fileName, String date, int epochSize){
		HashMap<String, EpochTMC> epochTMCPairs=new HashMap<String, EpochTMC>();
		
		String line="";
		try{
			//analyze 
			String filePath=Constants.PROBE_RAW_DATA+date+"/"+fileName;
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			int cnt=0;
			while ((line = br.readLine()) != null) {
				cnt+=1;
				String[] fields=line.split(",");
				if(fields.length!=Constants.IDX_TMC_POINT_LOC_CODE+1) continue;
				
				String tmc=fields[Constants.IDX_CTY_CODE]+fields[Constants.IDX_TABLE_ID];
				if(fields[Constants.IDX_TMC_DIR].equals("+")) tmc+="P";
				else tmc+="N";
				tmc+=fields[Constants.IDX_TMC_POINT_LOC_CODE];
				
				DateFormat simpDateFormat=new SimpleDateFormat("YYYY-MM-DD HH:MM:SS");
				Date systeTimestamp=simpDateFormat.parse(fields[Constants.IDX_SYS_DATE]);
				int epochIdx=(systeTimestamp.getHours()*3600+systeTimestamp.getMinutes()*60+systeTimestamp.getSeconds())
						/epochSize;
				
				String id=date+"-"+epochIdx+"-"+tmc;
				
				EpochTMC epochTMC;
				//a new tmc-epoch pair
				if(!epochTMCPairs.containsKey(id)){
					epochTMC= new EpochTMC(date, tmc, epochIdx);
					epochTMCPairs.put(id,epochTMC);
				}else{
					epochTMC=epochTMCPairs.get(id);
				}
				epochTMC.noOfProbes+=1;
				epochTMC.vehicleSet.add(fields[Constants.IDX_PROBE_ID]);
				epochTMC.providerSet.add(fields[Constants.IDX_VENDOR_DESC]);
			}
			br.close();
			
			System.out.println("line cnt of the raw probe data file "+fileName+ " = "+cnt);
		}catch(Exception ex){
			System.out.println("line ="+line);
			ex.printStackTrace();
		}
		return epochTMCPairs;
	}
	
	public void v2ReadStatResults(HashMap<String,TMC> tmcs, HashMap<String, MarketV2> markets,HashMap<String,String> tmcToMarket,String filepath){
		String tmc=null;
		try{
			Scanner sc=new Scanner(new File(filepath));
			while(sc.hasNextLine()){
				String[] fields=sc.nextLine().split(",");
				if(fields.length!=4) continue;
				//System.out.println(Arrays.toString(fields));
				tmc=fields[0].trim();
				
				if(tmcToMarket.containsKey(tmc)
						&&tmcs.containsKey(tmc)
						&&tmcs.get(tmc).maxAccessControl){
					
					if(markets.containsKey(tmcToMarket.get(tmc))){
						MarketV2 market=markets.get(tmcToMarket.get(tmc));
						XAXisMetric metric=market.densityMetrics;
						int probeCnt=Integer.parseInt(fields[1]);
						metric.sumOfProbes+=probeCnt;
						if(probeCnt>Constants.PROBE_CNT_THRSHOLD) metric.noOfTMCsWithProbeCntOverThreshold+=1;
						int vehicleCnt=Integer.parseInt(fields[2]);
						metric.sumOfVehicles+=vehicleCnt;
						if(vehicleCnt>Constants.VEHICLE_CNT_THRESHOLD) metric.noOfTMCsWithVehicleCntOverThreshold+=1;
						metric.sumOfProviders+=Integer.parseInt(fields[3]);
					}
				}
			}
			sc.close();
		}catch(Exception ex){
			System.out.println("tmc="+tmc);
			ex.printStackTrace();
		}
	}
	
	public HashMap<String, MarketV2> readGroundTruth(String date){
		HashMap<String, MarketV2> markets=new HashMap<String, MarketV2>();
		try{
			Scanner sc=new Scanner(new File(Constants.PROBE_STAT_DATA+date+"/ground_truth_"+date+".txt"));
			while(sc.hasNextLine()){
				String[] fields=sc.nextLine().split(",");
				
				if(!fields[Constants.IDX_TIME_PERIOD].equals("ALL DAY")) continue;
				
				String marketName=fields[Constants.IDX_MARKET].replace("\\", "");
				MarketV2 market;
				if(!markets.containsKey(marketName)){
					market=new MarketV2(marketName, date);
					markets.put(marketName, market);
				}else{
					market=markets.get(marketName);
				}
				
				String engine=fields[Constants.IDX_ENGINE_TYPE].trim(), timePeriod=fields[Constants.IDX_TIME_PERIOD];
				ConditonV2 condition=new ConditonV2(engine, timePeriod, "ALL");
				market.qualityMetrics.put(condition.toString(), new YAxisMetric(Double.parseDouble(fields[Constants.IDX_ROAD_CONDITION_ALL])));
				ConditonV2.allConditonV2s.add(condition.toString());
				
				condition=new ConditonV2(engine, timePeriod, "Congestion");
				market.qualityMetrics.put(condition.toString(), new YAxisMetric(Double.parseDouble(fields[Constants.IDX_ROAD_CONDITION_CONGESTION])));
				ConditonV2.allConditonV2s.add(condition.toString());
			}
			System.out.println("# of markets="+markets.size());
			sc.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return markets;
	}
	
	public HashMap<String, String> loadTMCToMarket(String extendCountryCode, HashMap<String, MarketV2> markets, HashMap<String, TMC> tmcs){
		HashMap<String, String> tmcToMarket=new HashMap<String, String>();
		try{
			BufferedReader br;
			FileWriter fw=null;
			File areaTMCToMarket=new File(Constants.MAP_DATA+extendCountryCode+"_tmcToMarket.txt");
			if(areaTMCToMarket.exists()){
				br = new BufferedReader(new FileReader(areaTMCToMarket));
				
			}else{
				br = new BufferedReader(new FileReader(Constants.MAP_DATA+"tmcToMarket.txt"));
				fw=new FileWriter(areaTMCToMarket);
				System.out.println("fw is not null.");
			}
			
			String line;
			while ((line = br.readLine()) != null) {
				String[] fields=line.split(",");
				if(!fields[1].equals(extendCountryCode)) continue;
				if(fw!=null){
					fw.write(line+"\n");
					//System.out.println(line);
				}
				int marketNameIdx=-1;
				switch (extendCountryCode) {
				case "A0":
					marketNameIdx=14;
					break;
				default:
					break;
				}
				String tmc=fields[0],  market=fields[marketNameIdx].replace("\\", "");
				tmcToMarket.put(tmc, market);
				
				//update market info.
				if(markets!=null&&markets.containsKey(market)){
					MarketV2 marketV2=markets.get(market);
					marketV2.tmcs.add(tmc);				
					if(tmcs!=null&&tmcs.containsKey(tmc)){
						marketV2.miles+=tmcs.get(tmc).miles;
					}
				}
			}
			br.close();
			if(fw!=null) fw.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return tmcToMarket;
	}
	
	public HashMap<String, MarketV2> loadMarketV2s(){
		HashMap<String, MarketV2> marketV2s=new  HashMap<String, MarketV2>();
		try{
			Scanner sc=new Scanner(new File( Constants.BIN_FOLDER+"v2.csv"));
			sc.nextLine(); //read off the header
			while(sc.hasNextLine()){
				String[] fields=sc.nextLine().split(",");
				marketV2s.put(fields[0], new MarketV2(fields));
			}
			sc.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return marketV2s;
	}
	
	public HashMap<String, MarketV3> loadMarketV3s(){
		HashMap<String, MarketV3> marketV3s=new  HashMap<String, MarketV3>();
		try{
			Scanner sc=new Scanner(new File( Constants.BIN_FOLDER+"v3.csv"));
			sc.nextLine(); //read off the header
			while(sc.hasNextLine()){
				String[] fields=sc.nextLine().split(",");
				marketV3s.put(fields[0], new MarketV3(fields));
			}
			sc.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		System.out.println(marketV3s.size()+" markets retrieved");
		return marketV3s;
	}
	
	public HashMap<String, TMC> loadTMC(String extendCountryCode){
		HashMap<String, TMC> tmcs=new HashMap<String, TMC>();
		try{
			BufferedReader br;
			FileWriter fw=null;
			File areaTMCToMarket=new File(Constants.MAP_DATA+extendCountryCode+"_tmc_attribute.txt");
			if(areaTMCToMarket.exists()){
				br = new BufferedReader(new FileReader(areaTMCToMarket));
			}else{
				br = new BufferedReader(new FileReader(Constants.MAP_DATA+"tmc_attribute.txt"));
				fw=new FileWriter(areaTMCToMarket);
				System.out.println("fw is not null.");
			}
			
			String line;
			while ((line = br.readLine()) != null) {
				String[] fields=line.split(",");
				if(!fields[94].equals(extendCountryCode)) continue;
				if(fw!=null){
					fw.write(line+"\n");
					//System.out.println(line);
				}
				String tmc=fields[0];
				double miles=Double.parseDouble(fields[1]);
				boolean minAC=fields[42].equals("Y")?true:false,maxAC=fields[68].equals("Y")?true:false;
				tmcs.put(tmc, new TMC(tmc, miles, minAC, maxAC, extendCountryCode));
			}
			br.close();
			if(fw!=null) fw.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return tmcs;
	}
	
	public void mergeResultFiles(final String version){
		try{
			File[] resultFiles=new File(Constants.BIN_FOLDER).listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					//System.out.println(name);
					if(name.startsWith(version+"_")&&name.endsWith(".csv")) return true;
					return false;
				}
			});
			FileWriter fw=new FileWriter(Constants.BIN_FOLDER+version+".csv");
			Scanner sc;
			for(int i=0;i<resultFiles.length;i++){
				sc=new Scanner(resultFiles[i]);
				System.out.println(resultFiles[i].getName());
				if(i>0) sc.nextLine(); //read off the header
				while(sc.hasNextLine()){
					fw.write(sc.nextLine()+"\n");
				}
				sc.close();	
			}
			fw.close();			
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	
	public void loadStatFilesToNetezza(){
		try{
			Scanner sc=new Scanner(new File(Constants.PROJECT_FOLDER+"temp.txt"));
			HashMap<String, Integer> probeCnts=new HashMap<String, Integer>();
			HashMap<String, HashSet<String>> vehicleCnts=new HashMap<String, HashSet<String>>();
			while(sc.hasNextLine()){
				String[] fields=sc.nextLine().split(",");
				//System.out.println(Arrays.toString(fields));
				String tmc=fields[0];
				probeCnts.put(tmc, Integer.parseInt(fields[1]));
				vehicleCnts.put(tmc, new HashSet<String>());
				vehicleCnts.get(tmc).add(fields[Constants.IDX_PROBE_ID]);
			}
			Connection netezza=initilize("Netezza");
			sc.close();
			netezza.close();
			//insertTMCDensityData(netezza, probeCnts, vehicleCnts);
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	
	public void insertTMCDensityData(Connection netezza, HashMap<String, Integer> probeCnts, HashMap<String, HashSet<String>> vehicleCnts){
		try{
			PreparedStatement insertRowTMCDensityTableQuery=netezza.prepareStatement("INSERT INTO TMC_DENSITY_AMERICA VALUES(?, ?, ?);");
			int noOfInsertions=0, batchSize=2;
			for(String tmc: probeCnts.keySet()){
				insertRowTMCDensityTableQuery.setInt(2, probeCnts.get(tmc));
				insertRowTMCDensityTableQuery.setInt(3, vehicleCnts.get(tmc).size());
				insertRowTMCDensityTableQuery.setString(1, tmc);
				//insertRowTMCDensityTableQuery.executeUpdate();
				//execute in batch
				/**
				 * can only executeBatch() once, second time it throws an error
				 */
				insertRowTMCDensityTableQuery.addBatch();
				if(++noOfInsertions%batchSize==0){
					insertRowTMCDensityTableQuery.executeBatch(); 
				}
			}
			insertRowTMCDensityTableQuery.executeBatch();
			insertRowTMCDensityTableQuery.close();
			System.out.println(noOfInsertions+" records have been inserted");
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
	}
	
	
	
	/**
	 * @return initialize the database connections
	 */
	public Connection initilize(String databaseBrand){
		Connection conn=null;
		try {
			if(databaseBrand.equals("Oracle")){
				 DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
				  
				conn= DriverManager.getConnection(
				  "jdbc:oracle:thin:@"+Constants.ORACLE_SOCKET+"/"+Constants.ORACLE_DBNAME,
				     Constants.ORACLE_ACCOUNT, Constants.ORACLE_PASSWORD );
			}else{
				if(databaseBrand.equals( "Netezza")){
					Class.forName("org.netezza.Driver");
					conn = DriverManager.getConnection(
							"jdbc:netezza://"+Constants.NETEZZA_SOCKET+"/"+Constants.NETEZZA_DBNAME
							, Constants.NETEZZA_ACCOUNT, Constants.NETEZZA_PASSWORD);
				}
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return conn;
	}
	
	public void analyzeMarket(Connection conn){
		
		//get the predicated speed table
		
		
		//compare the predicated speed and ground truth table
		
		
		try{
			Statement sm = conn.createStatement();
			
			String sqlQuery=//"select count(*) from("+
			"SELECT to_char(system_date, 'yyyy-mm-dd hh24:mi:ss') , to_char(sample_date, 'yyyy-mm-dd hh24:mi:ss') , probe_data_provider_desc , probe_id , speed ,"
			+" ebu_country_code || lpad(location_table_nr,2,'0') ||"
			+ "case when TMC_path_direction = '+' then 'P' when TMC_path_direction = '-' then 'N' else TMC_path_direction end"
			+ "|| point_location_code as tmc\n "
			+ "from\n probe.PROBE_DATA_RAW_ARCHIVE PARTITION(P20131212) t1\n"
			+ "WHERE\n mapped_flag = 'Y' and ebu_country_code = '1'";//) t2";
			
			String sqlQuery1="select * from probe.PROBE_DATA_RAW_ARCHIVE PARTITION(P20131212)";
			
			ResultSet rs = sm.executeQuery(sqlQuery);
			ResultSetMetaData rsmd = rs.getMetaData();
			
			
			int cnt=0;
			while(rs.next()){
				cnt++;
			}
			System.out.println(cnt);
			
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
}
