package com.here.traffic.quality.correlation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.Variance;

import sun.nio.cs.MS1250;
import sun.tracing.ProbeSkeleton;

import com.here.traffic.quality.correlation.ds.TMC;
import com.here.traffic.quality.correlation.ds.v2.ConditionV2;
import com.here.traffic.quality.correlation.ds.v2.MarketV2;
import com.here.traffic.quality.correlation.ds.v2.XAxisMetric;
import com.here.traffic.quality.correlation.ds.v2.YAxisMetric;
import com.here.traffic.quality.correlation.ds.v3.MarketV3;
import com.here.traffic.quality.correlation.ds.v3.XYMetrics;
import com.here.traffic.quality.correlation.ds.v4.EpochTMC;
import com.here.traffic.quality.correlation.ds.v4.Probe;
import com.here.traffic.quality.correlation.ds.v4.Probe.SOURCE;
import com.here.traffic.quality.correlation.ds.v5.DensityBucket;

import org.apache.commons.lang3.*;;

public class CorrelationAnalysis {
	public static Mean mean=new Mean();
	public static Variance var=new Variance();
	
	public static HashSet<String> PROVIDER_BLACKLIST_BAD_SPEED_DISTRIBUTION=new HashSet<String>();
	public static HashSet<String> PROVIDER_BLACKLIST_BAD_HEADING_DISTRIBUTION=new HashSet<String>();
	
	
	static{
		String[] providers={  
				"Fleetmatics", "GPS Insight",  "Navigon",  "Navigon_global_pb",  "Telecomsys",  "Teletrac"
				, "Teletrac_na_pb", "Telogis","Telogis_na_pb", "Trimble"};
		for(String p: providers) PROVIDER_BLACKLIST_BAD_SPEED_DISTRIBUTION.add(p.toUpperCase());
		
		providers=new String[]{  
	"ADAC-GERMANY-PROBE", "CMA", "MIX-GBR",		"MIX_GBR_PB",		"PUNCH",		"ECOTELEMATICS",		"MASTERNAUT_GBR_PB",
	"FRAMELOGIC",		"MASTERNAUT_SPAIN",		"CYBIT_EU_PB",		"INOSAT",		"FINDER",		"FROTCOM",		"CARRIERWEB_EU_PB",
	"CMA_EU_PB",		"CARRIERWEB",		"VEHCO_EU_PB",		"MOBIVISION",		"PUNCH_EU_PB",		"ADAC_EU_PB",		"SATKO"};
		for(String p: providers) PROVIDER_BLACKLIST_BAD_HEADING_DISTRIBUTION.add(p.toUpperCase());
	}
	
	
	public static void main(String[] args) {
		new CorrelationAnalysis().main();
	}
	
	public static String analysisVersion;

	//TODO main
	public void main(){
		//secondAttempt();
		//thirdAttempt();
		
		//addProbeCntToMarketv3();
		
		v4OutputStatResults();
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
	
	private boolean isFull(ArrayList<ArrayList<EpochTMC>> pairsFittingProfile, int targetNOofPairs){
		for(ArrayList<EpochTMC> list: pairsFittingProfile){
			if(list.size()<targetNOofPairs) return false;
		}
		return true;
	}
	
	private void writeEpochTMCPairsToFile(ArrayList<EpochTMC> pairs, String filepath, boolean  findTMCsFittingtheProfile){
		//sort epochTMC based on time
		Collections.sort(pairs, new Comparator<EpochTMC>() 
		{
		    public int compare(EpochTMC o1, EpochTMC o2) 
		    {
		       if(o1 instanceof EpochTMC && o2 instanceof EpochTMC) 
		       {
		         return ((EpochTMC)o1).epochIdx-((EpochTMC)o2).epochIdx;
		       } 
		       return 0;    
		    }
		});					
		try{
				FileWriter fw=new FileWriter(filepath);
				for(EpochTMC epochTMC: pairs){
					fw.write(epochTMC.toString());//only write epoch-tmc pairs with ground truth 
					if(findTMCsFittingtheProfile){
						Collections.sort(epochTMC.probes);
						if(epochTMC.probes.size()>0)
						fw.write(","+epochTMC.probes.get(0).timestamp.split(" ")[1]+","+epochTMC.probes.get(epochTMC.probes.size()-1).timestamp.split(" ")[1]);
						
						fw.write("\n"+epochTMC.probes+"\n");
					}else{
						fw.write('\n');
					}
				}
				fw.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	
	public int loadGroundTruthAndBuildEpochTMCs(String filePath, HashMap<String, EpochTMC> epochTMCs, String tableID, String engineType
			, String date, String country){
		String line;
		int lineCnt=0;
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(filePath));
			while((line=br.readLine())!=null){
				lineCnt+=1;
				String[] fields=line.split(",");
				String tmc=fields[10];
				if(tableID.length()>0&&!tmc.startsWith(tableID)) continue;
				//get the epochIdx
				//int epochIdx=Integer.parseInt(fields[8])/180; //each epoch is 3 minutes
				
				/*Date systeTimestamp=null;
				try{
					DateFormat simpDateFormat=new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");//must be small letters
					systeTimestamp=simpDateFormat.parse(fields[14]);//GMT end time
				}catch(Exception ex){
					DateFormat simpDateFormat=new SimpleDateFormat("yyyy-mm-dd hh:mm");//must be small letters
					systeTimestamp=simpDateFormat.parse(fields[14]);//GMT end time
					//System.out.println(fields[14]+" "+line);
					//continue;
				}*/
				int secondsOfDay=CommonUtils.HMSToSeconds(fields[14].split(" ")[1]);
						//systeTimestamp.getHours()*3600+systeTimestamp.getMinutes()*60+systeTimestamp.getSeconds();
				int epochIdx=secondsOfDay/180;
				
				double error=Double.parseDouble(fields[30]);//capped difference
				String engine=fields[11];
				String isHyw=fields[12];
				String flowType=fields[35];//36
				if(!isHyw.equals("Y")
						||!engine.equals(engineType)) continue;
				//(!engine.equals("HTTM")&&!engine.equals("HALO"))
				
				String condition=engine+"-"+flowType+"-"+isHyw;	
				String id=date+"-"+epochIdx+"-"+tmc;//+"-"+condition;
				Double groudTruthSpeed=Double.parseDouble(fields[27]);
				EpochTMC epochTMC=new EpochTMC(date, tmc, epochIdx, error, condition, groudTruthSpeed);
				
				
				if(epochTMCs.containsKey(id)){
					if(!epochTMCs.get(id).condition.equals(condition)){
						/*System.out.println(id+"  epoch: "+fields[8]);
						System.out.println(epochTMCs.get(id));
						System.out.println(line);*/
					}
				}
				epochTMCs.put(id, epochTMC);
				epochTMC.error=error;
			}//end of parsing a ground truth file
			System.out.println("read "+lineCnt+" lines; "+epochTMCs.size()+" pairs ("+engineType+") loaded from groundtruth file  "+ country+"-"+date);
			br.close();			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return lineCnt;
	}
	
	public void parseRawProbleFile(String filePath, boolean spdFiltering, double spdFilterThrshold,
		String tableID	){
		
	}
	
	public void v4OutputStatResults(){
		
		SOURCE probeSource=SOURCE.ARCHIVE;
		
		/**
			 * Specify constraints in order to extract specific tmc-epoches
			 */
			boolean findTMCsFittingtheProfile=false;
			int[] groundTruthSpdRange={55, 100};
			int[] probeSpdStdRange={10, 30};
			//int[] groundTruthMinusProbeSpdMean={10, 60};
			int[][] densityRange={{1,10},{20,40},{40,200}};
			int targetNOofPairs=10;
			ArrayList<ArrayList<EpochTMC>> pairsFittingProfile=new ArrayList<ArrayList<EpochTMC>>();
			for(int i=0;i<densityRange.length;i++){
				pairsFittingProfile.add(new ArrayList<EpochTMC>());
			}
			
		
		boolean spdFiltering=false,
				coutingWindowShifting=true;
		double spdFilterThrshold=0;//0,10
		
		if(spdFiltering&&coutingWindowShifting) analysisVersion="v5";
		else{
			if(!spdFiltering&&!coutingWindowShifting) analysisVersion="v4";
			else{
				if(!spdFiltering&&coutingWindowShifting) analysisVersion="v6";
				else analysisVersion="v7";
			}
		}
		 
		//"20140320", "20131212","20131213","20140205",
		String[] dates={"20140320"};//,};//"20131212","20131213","20131220","20140205"
		String[] countries={"US"};//US, France
		String[] engines={"HALO"};//"HTTM", "HALO"
		String[] tableIDs={"107","108","109"};// "101","102","103" "128","129","130"
		
		FileWriter fw;
		HashMap<String, TMC> tmcAttr=null;		
				
		try{
			for(String tableID: tableIDs){
				//parse ground truth files
				for(String country: countries){
					int endingBatch=11;
					switch(country){
					case "US":
						tmcAttr=TMC.loadTMC("US");
						endingBatch=11;
						break;
					case "France":
						tmcAttr=TMC.loadTMC("France");
						endingBatch=1;
					}
					
					boolean isFull=false;
					for(String date: dates){
						for(String engineType: engines){
							//load epochTMCs from ground truth
							HashMap<String, EpochTMC> epochTMCs=new HashMap<String, EpochTMC>(); 
							String groudtruthFilePath=Constants.GROUND_TRUTH_DATA+country+"/groundtruth_"+date+"_"+country+".txt";
						
							int lineCnt=0;
							
							lineCnt+=loadGroundTruthAndBuildEpochTMCs(groudtruthFilePath, epochTMCs, tableID, engineType, date, country);
							
							/**
							 * read raw probe data file and count
							 */
							//if(true) return;
							int cnt=0;
							if(probeSource==SOURCE.ARCHIVE){//source is archive
								int startingBatch;
								if(findTMCsFittingtheProfile)	startingBatch=1;
								else startingBatch=0;
								
								if(tableID.length()>0){
									startingBatch=(Integer.parseInt(tableID)-101)/3;
									endingBatch=startingBatch+1;
								}
								for(int batch=startingBatch;batch<endingBatch;batch++){//TODO change this based on the country
									String batchString=(3*batch+1)+","+(3*batch+2)+","+(3*batch+3);
									if(country.equals("France")){
										batchString="F32";
										if(batch==1) break;
									}
								
									String rawProbeFilePath=Constants.PROBE_RAW_DATA+country+"/"+date+"/"+date+"_"+batchString+"_probe.csv";
									
									BufferedReader brr = new BufferedReader(new FileReader(rawProbeFilePath));
									int probeCnt=0, updateProbeCnt=0;
									
									
									//start reading one probe data file
									HashMap<String, EpochTMC> pairsVisitedInThisFile=new HashMap<String, EpochTMC>();
									String newLine;
									String[] fields;
									
									while ((newLine = brr.readLine()) != null) {
										try{
											probeCnt+=1;
											fields=newLine.split(",");
											if(fields.length!=Constants.RAW_PROBE_IDX_TMC_POINT_LOC_CODE+1) continue;
											
											//throw away low speed data
											double speed=Double.parseDouble(fields[Constants.RAW_PROBE_IDX_SPEED]);
											if(spdFiltering&&speed<=spdFilterThrshold){
												continue;
											}
											
											String provider=fields[Constants.RAW_PROBE_IDX_VENDOR_DESC].toUpperCase();
											String vehicle=fields[Constants.RAW_PROBE_IDX_VEHICLE_ID].toUpperCase();
											if(PROVIDER_BLACKLIST_BAD_HEADING_DISTRIBUTION.contains(provider)
												||PROVIDER_BLACKLIST_BAD_SPEED_DISTRIBUTION.contains(provider)){
												//continue;
											}
											
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
											if(!tmcAttr.containsKey(tmc)) continue; //TMC table does not contains this tmc
											if(tableID.length()>0&& !tmc.startsWith(tableID)) continue;
											
											//DateFormat simpDateFormat=new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");//must be small letters
											//Date systeTimestamp=simpDateFormat.parse(fields[Constants.RAW_PROBE_IDX_SAMPLE_DATE]);
											
											int delay=5;//minutes
											int lookback=10; //minutes
											int secondsOfDay=CommonUtils.HMSToSeconds(fields[Constants.RAW_PROBE_IDX_SAMPLE_DATE].split(" ")[1]);
													//systeTimestamp.getHours()*3600+systeTimestamp.getMinutes()*60+systeTimestamp.getSeconds();
											Probe probe=new Probe(fields[Constants.RAW_PROBE_IDX_SAMPLE_DATE], 
													Double.valueOf(fields[Constants.RAW_PROBE_IDX_SPEED])*0.621371 //km to mile
													, provider, vehicle);
											
											
											int startEpochIdx;
											int endEpochIdx;
											if(coutingWindowShifting){
												startEpochIdx=(secondsOfDay+delay*60)/180;
												endEpochIdx=(secondsOfDay+(delay+lookback)*60)/180;
											}else{
												startEpochIdx=secondsOfDay/180;
												endEpochIdx=startEpochIdx+1;
											}
											for(int epochIdx=startEpochIdx;epochIdx<endEpochIdx;epochIdx++){
												String pairID=date+"-"+epochIdx+"-"+tmc;
												
												EpochTMC epochTMC;
												//a new tmc-epoch pair
												if(!epochTMCs.containsKey(pairID)){
													continue;
												}else{
													updateProbeCnt++;
													epochTMC=epochTMCs.get(pairID);
												}
												if(!pairsVisitedInThisFile.containsKey(pairID)){
													pairsVisitedInThisFile.put(pairID, epochTMC);
												}
												
												epochTMC.noOfProbes+=1;
												//epochTMC.noOfProbesPerMile=epochTMC.noOfProbes/tmcAttr.get(epochTMC.tmc).miles;
												epochTMC.vehicleSet.add(fields[Constants.RAW_PROBE_IDX_VEHICLE_ID]);
												epochTMC.providerSet.add(fields[Constants.RAW_PROBE_IDX_VENDOR_DESC]);
												
												if(epochTMC.probeIDs.contains(probe.id)){
													continue;
												}
												epochTMC.probes.add(probe);
												epochTMC.probeIDs.add(probe.id);
												
												
												/**
												 * debug
												 */
												if(epochTMC.tmc.equals("106P05000")&&epochTMC.epochIdx==408){
													//System.out.println(line);
													//System.out.println("no.of probs:"+epochTMC.noOfProbes);
												}
											}
										}catch(Exception ex){
											System.out.println(newLine);
											ex.printStackTrace();
										}						
									}//end of parsing a raw probe data file
									
									
									//write to a file
									for(EpochTMC epochTMC: pairsVisitedInThisFile.values()){
										if(epochTMC.noOfProbes>0){  
											cnt+=1;
											//normalize the count by miles
											epochTMC.noOfProbesPerMile=epochTMC.noOfProbes/tmcAttr.get(epochTMC.tmc).miles;
											double[] array=new double[epochTMC.probes.size()];
											for(int i=0;i<epochTMC.probes.size();i++){
												array[i]=epochTMC.probes.get(i).speed;
											}
											epochTMC.probeSpeedMean=mean.evaluate(array);
											epochTMC.probeSpeedStd=Math.sqrt(var.evaluate(array, epochTMC.probeSpeedMean) );								
											
											if(findTMCsFittingtheProfile){
												for(int di=0;di<densityRange.length;di++){
													if(epochTMC.probeSpeedStd>=probeSpdStdRange[0]&&epochTMC.probeSpeedStd<probeSpdStdRange[1]
															&&epochTMC.noOfProbesPerMile>=densityRange[di][0]&&epochTMC.noOfProbesPerMile<densityRange[di][1]
																	&&epochTMC.groundTruthSpeed>=groundTruthSpdRange[0]&&epochTMC.groundTruthSpeed<groundTruthSpdRange[1]
																			&&Math.abs(epochTMC.error)>=10
																){
																	pairsFittingProfile.get(di).add(epochTMC);
																	break;
																}
																
												}
												isFull=isFull(pairsFittingProfile, targetNOofPairs);
												if(isFull) break;
											}
										}
									}
									System.out.println(country+"-"+date+"-"+batchString+"  has : "+probeCnt+" probes; "+updateProbeCnt+" are mapped to the groundtruth pairs");//no of pairs read out
									brr.close();

									System.out.println("# of pairsFittingProfile: "+pairsFittingProfile.get(0).size()
											+"\t"+pairsFittingProfile.get(1).size()+"\t"+pairsFittingProfile.get(2).size());
									if(isFull) break;

								}
							}else{//source is qml file
								//TODO filepath 
								String qmlProbeFilePath=Constants.DATA_FOLDER+"/HALO_Parser/output_qml-2014-03-20-05-00-2014-03-21-05-00-fd445ee6-73b9-4d86-a290-a774c076a22d_HighwayTMCsOnly.csv";
								
								BufferedReader brr = new BufferedReader(new FileReader(qmlProbeFilePath));
								int probeCnt=0, updateProbeCnt=0;
								
								//start reading one probe data file
								HashMap<String, EpochTMC> pairsVisitedInThisFile=new HashMap<String, EpochTMC>();
								String newLine;
								String[] fields;
								brr.readLine();//read off the header line (the first line)
								while ((newLine = brr.readLine()) != null) {
									try{
										probeCnt+=1;
										fields=newLine.split(",");
										
										//TODO Change all the field index
										if(fields.length!=33) continue;
										String tmc=fields[1];
										if(!tmcAttr.containsKey(tmc)) continue; //TMC table does not contains this tmc
										
										//throw away low speed data
										double speed;
										if(fields[27].length()>0) speed=Double.parseDouble(fields[27])/1.609344; //speed in kph
										else{
											if(fields[26].length()>0) speed=Double.parseDouble(fields[26])/1.609344;
											else continue;
										}
										
										if(spdFiltering&&speed<=spdFilterThrshold){
											continue;
										}
										
										String dataType=fields[19];
										
										if(dataType.equals("probe")){
											
										}else{
											continue;
										}
										
										String provider=fields[20].toUpperCase();
										String vehicle=fields[23].toUpperCase();
										if(PROVIDER_BLACKLIST_BAD_HEADING_DISTRIBUTION.contains(provider)
											||PROVIDER_BLACKLIST_BAD_SPEED_DISTRIBUTION.contains(provider)){
											//continue;
										}
										
										if(tableID.length()>0&& !tmc.startsWith(tableID)) continue;
										//System.out.println();
										
									
										
										Probe probe=new Probe(fields[0].split("\\.")[0].replace('T', ' '), 
												speed
												, provider, vehicle);
										
										//DateFormat simpDateFormat=new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");//must be small letters
										//Date systeTimestamp=simpDateFormat.parse(fields[Constants.RAW_PROBE_IDX_SAMPLE_DATE]);
										
										int delay=5;//minutes
										int lookback=10; //minutes
										int secondsOfDay=CommonUtils.HMSToSeconds(fields[0].split("T")[1].split("\\.")[0]);
												//systeTimestamp.getHours()*3600+systeTimestamp.getMinutes()*60+systeTimestamp.getSeconds();

										int startEpochIdx;
										int endEpochIdx;
										if(coutingWindowShifting){
											startEpochIdx=(secondsOfDay+delay*60)/180;
											endEpochIdx=(secondsOfDay+(delay+lookback)*60)/180;
										}else{
											startEpochIdx=secondsOfDay/180;
											endEpochIdx=startEpochIdx+1;
										}
										for(int epochIdx=startEpochIdx;epochIdx<endEpochIdx;epochIdx++){
											String pairID=date+"-"+epochIdx+"-"+tmc;
											
											EpochTMC epochTMC;
											//a new tmc-epoch pair
											if(!epochTMCs.containsKey(pairID)){
												continue;
											}else{
												updateProbeCnt++;
												epochTMC=epochTMCs.get(pairID);
											}
											if(!pairsVisitedInThisFile.containsKey(pairID)){
												pairsVisitedInThisFile.put(pairID, epochTMC);
											}
											
											epochTMC.noOfProbes+=1;
											//epochTMC.noOfProbesPerMile=epochTMC.noOfProbes/tmcAttr.get(epochTMC.tmc).miles;
											epochTMC.vehicleSet.add(fields[Constants.RAW_PROBE_IDX_VEHICLE_ID]);
											epochTMC.providerSet.add(fields[Constants.RAW_PROBE_IDX_VENDOR_DESC]);
											
											if(epochTMC.probeIDs.contains(probe.id)){
												continue;
											}
											epochTMC.probes.add(probe);
											epochTMC.probeIDs.add(probe.id);
										}
									}catch(Exception ex){
										System.out.println(newLine);
										ex.printStackTrace();
									}						
								}//end of parsing a raw probe data file
								
								
								//calculate the probe speed mean and std for each pair
								for(EpochTMC epochTMC: pairsVisitedInThisFile.values()){
									if(epochTMC.noOfProbes>0){  
										cnt+=1;
										//normalize the count by miles
										epochTMC.noOfProbesPerMile=epochTMC.noOfProbes/tmcAttr.get(epochTMC.tmc).miles;
										double[] array=new double[epochTMC.probes.size()];
										for(int i=0;i<epochTMC.probes.size();i++){
											array[i]=epochTMC.probes.get(i).speed;
										}
										epochTMC.probeSpeedMean=mean.evaluate(array);
										epochTMC.probeSpeedStd=Math.sqrt(var.evaluate(array, epochTMC.probeSpeedMean) );
									}
								}
								System.out.println(country+"-"+date+"-"+"  has : "+probeCnt+" probes; "+updateProbeCnt+" are mapped to the groundtruth pairs");//no of pairs read out
								brr.close();
								System.out.println("# of pairsFittingProfile: "+pairsFittingProfile.get(0).size()
										+"\t"+pairsFittingProfile.get(1).size()+"\t"+pairsFittingProfile.get(2).size());
							}
							
							
							System.out.println("# of pairs is "+epochTMCs.size()+" # of pairs with rt probes is "+cnt+ "  percentage is "+String.format("%.2f", (cnt+0.0)/epochTMCs.size()) );
							
							//if(true) return;
							ArrayList<EpochTMC> pairs;
							String outputFilePath;
							outputFilePath=Constants.RESULT_DATA+analysisVersion+"/"+analysisVersion+"_"+date+"_";
							if(tableID.length()==0) outputFilePath+=country;
							else outputFilePath+=tableID;
							if(probeSource==SOURCE.QML) outputFilePath+="_qml";
							outputFilePath+=".csv";
							pairs=new ArrayList<EpochTMC>();
							for(EpochTMC epochTMC: epochTMCs.values()) pairs.add(epochTMC);
							writeEpochTMCPairsToFile(pairs, outputFilePath, findTMCsFittingtheProfile);
							
							
						}//end of parsing probe files for one day
						if(isFull) break;
					}	
				}
			}
			
			
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	
	public HashMap<String,ArrayList<EpochTMC>> v4ReadStatResult(String filepath, HashMap<String, ArrayList<EpochTMC>> epochTMCs, String version, String  date,
			String country, String engine, boolean directlyExtractedFromGroundtruthTable
			){

		return v4ReadStatResult(filepath, epochTMCs, null, version, date, country, engine, directlyExtractedFromGroundtruthTable);
	}
		
	
	
	public HashMap<String,ArrayList<EpochTMC>> v4ReadStatResult(
			String filepath,
			HashMap<String, ArrayList<EpochTMC>> epochTMCs, HashMap<String, TMC> tmcs,
			String version, String  date, String country, String engine, boolean directlyExtractedFromGroundtruthTable
		){
		
		//read stat file and analyze
		try{
			Scanner sc;
			int lineCnt=0;
			System.out.println(epochTMCs.keySet());
			if(!directlyExtractedFromGroundtruthTable){//stat file is generated by parsing raw probe data files

				sc=new Scanner(new File(filepath));
				while(sc.hasNextLine()){
					lineCnt++;
					String line=sc.nextLine();
					String[] fields=line.split(",");
					
					//if(tmcs!=null&&tmcLanes.containsKey(fields[0])) continue;//not include in the tmcLane hashtable
					
					EpochTMC epochTMC=new EpochTMC(fields[0], fields[2], Integer.parseInt(fields[1]));
					epochTMC.condition=fields[3];
					if(!epochTMC.condition.startsWith(engine)){
						continue;
					}
					epochTMC.noOfProbes=Integer.parseInt(fields[4]);
					epochTMC.noOfProbesPerMile=Double.parseDouble(fields[5]);
					
					epochTMC.probeSpeedMean=Double.parseDouble(fields[6]);
					epochTMC.probeSpeedStd=Double.parseDouble(fields[7]);
					
					epochTMC.providerSetSize=Integer.parseInt(fields[fields.length-3]);
					
					epochTMC.groundTruthSpeed=Double.parseDouble(fields[fields.length-2]);
					epochTMC.error=Double.parseDouble(fields[fields.length-1]);
					
					int noOfConditions=epochTMCs.size();
					
					String key;
					switch (noOfConditions) {
					case 1:
						epochTMCs.get("All").add(epochTMC);
						break;
					case 2:
						if(epochTMC.condition.contains("Free")){
							epochTMCs.get("Free").add(epochTMC);
						}
						else{
							epochTMCs.get("Congestion").add(epochTMC);
						}
						break;
					case 4://HOVorExpressLane
						if(epochTMC.condition.contains("Free")) key="Free-";
						else key="Congestion-";
						boolean speicalLane=tmcs.get(epochTMC.tmc).maxCarpoolLane|tmcs.get(epochTMC.tmc).maxExpressLane;
						if(speicalLane) key+="with HOVorExpressLane";
						else key+="w/o HOVorExpressLane";									
						//System.out.println("key="+key);
						epochTMCs.get(key).add(epochTMC);
						break;
					case 10://# of lanes
						if(epochTMC.condition.contains("Free")) key="Free-";
						else key="Congestion-";
						int maxNoOfLane=tmcs.get(epochTMC.tmc).maxNoOfLane;
						if(maxNoOfLane>2&&maxNoOfLane<6) key+="="+maxNoOfLane;
						else{
							if(maxNoOfLane<=2) key+="<=2";
							else key+=">=6";
						}
						//System.out.println("key="+key);
						epochTMCs.get(key).add(epochTMC);
						break;
					case 9://speed split case
						double groudTruthSpd=epochTMC.groundTruthSpeed;
						
						//get the speed intervals
						double[] spdBoundaries=new double[epochTMCs.size()];
						int i=0;
						for(String spd: epochTMCs.keySet()){
							spdBoundaries[i++]=Double.parseDouble(spd);
						}
						Arrays.sort(spdBoundaries);
						//System.out.println(epochTMCs.keySet());
						//System.out.println(Arrays.toString(spdBoundaries));
						
						//find the speed interval to which the epochTMCPair belongs
						//find the largest speed that smaller than the groundtruth speed
						int l=-1, r=spdBoundaries.length;
						while(l+1!=r){
							int m=l+(r-l)/2;
							if(groudTruthSpd<spdBoundaries[m]) r=m;
							else l=m;
						}
						if(l>0&&spdBoundaries[l]==groudTruthSpd) l-=1;
						
						//System.out.println("spd:"+groudTruthSpd+" group:"+l);
						//add the pair to the corresponding group
						epochTMCs.get(String.valueOf(l*10)).add(epochTMC);
						break;
					default:
						break;
					}
				}
			}else{//stat file directly extracted from ground truth table
				sc=new Scanner(new File(Constants.RESULT_DATA+version+"/gt_table_"+date+"_"+country+"_Congestion.csv"));
				while(sc.hasNextLine()){
					lineCnt++;
					String line=sc.nextLine();
					String[] fields=line.split(",");
					EpochTMC epochTMC=new EpochTMC(fields[0], fields[1], Integer.parseInt(fields[2])/180);
					epochTMC.condition="HTTM-"+fields[3]+"-Y";
					
					epochTMC.noOfProbes=Integer.parseInt(fields[4]);
					epochTMC.noOfProbesPerMile=Double.parseDouble(fields[5]);
					epochTMC.error=Math.abs(Double.parseDouble(fields[fields.length-1]) );
				
					if(epochTMC.condition.contains("Free")) epochTMCs.get("Free").add(epochTMC);
					else epochTMCs.get("Congestion").add(epochTMC);
				}
			}			
			System.out.println(date+" "+country+" line cnt:"+lineCnt);
			for(String trafCond: epochTMCs.keySet()){
				System.out.println("Condition : "+trafCond+" , #OfPairs:"+ epochTMCs.get(trafCond).size());
			}
			sc.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return epochTMCs;
	}
	
	public void v4Attempt(){
		/**
		 * Set conditions
		 */
		boolean noTrafficConditionSplit=false;
		boolean trafficConditionDichotomy=true;//, false};
		boolean laneBased=false;//, true};
		boolean carpoolExpressLane=false;
		boolean samePopulationForBuckets=false;
		
		SOURCE probeDataSource=SOURCE.ARCHIVE;
		
		String[] tableIDs={"107","108","109"};//"107""101","102","103" "128","129","130"  //produce results for a particular market, e.g. "107" means Chicago
		
		
		/**
		 * data source parameters
		 */
		String[] dates={"20140320"};//,"20131220","20140205"};//"20131212","20131213","20131220","20140205"
		String country="US";//US, France
		analysisVersion="v6";//"v4","v5","v6"
		boolean plotting=false;//true, false;
		int minNOPairsInOneBucket=200; //default : 200
	
		HashMap<String, ArrayList<EpochTMC>> allPairs;
		HashMap<String, TMC> tmcs=TMC.loadTMC(country);
		
		for(String tableID: tableIDs){
			/***
			 * put pairs into different groups based on traffic conditions
			 */
			allPairs=new HashMap<String, ArrayList<EpochTMC>>();
			
			if(noTrafficConditionSplit){
				allPairs.put("All", new ArrayList<EpochTMC>());
			}else{
				if(trafficConditionDichotomy){
					if(laneBased){//build lane based conditions
						for(int i=2;i<=6;i++){
							switch(i){
							case 2:
								allPairs.put("Free-<="+i, new ArrayList<EpochTMC>());
								allPairs.put("Congestion-<="+i, new ArrayList<EpochTMC>());
								break;
							case 6:
								allPairs.put("Free->="+i, new ArrayList<EpochTMC>());
								allPairs.put("Congestion->="+i, new ArrayList<EpochTMC>());
								break;
							default:
								allPairs.put("Free-="+i, new ArrayList<EpochTMC>());
								allPairs.put("Congestion-="+i, new ArrayList<EpochTMC>());
								break;
							}
							
						}
					}else{
						if(carpoolExpressLane){
							allPairs.put("Free-with HOVorExpressLane", new ArrayList<EpochTMC>());
							allPairs.put("Congestion-with HOVorExpressLane", new ArrayList<EpochTMC>());
							allPairs.put("Free-w/o HOVorExpressLane", new ArrayList<EpochTMC>());
							allPairs.put("Congestion-w/o HOVorExpressLane", new ArrayList<EpochTMC>());
						}else{
							allPairs.put("Free", new ArrayList<EpochTMC>());
							allPairs.put("Congestion", new ArrayList<EpochTMC>());
						}
					}
				}else{
					for(int i=0;i<=8;i++){
						allPairs.put(String.valueOf(i*10), new ArrayList<EpochTMC>());//i*10 is speed range
					}
				}
			}
			
			
			
			//print out Title of the stats
			String title="\t All Markets in "+country+": HTTM-";
			title+=" on ";	
			for(String date: dates){
				title+=date+" ";
				boolean directedExtractedFromGroundtruthTable=false;
				
				String filepath;
				
				//filepath=Constants.RESULT_DATA+analysisVersion+"/"+analysisVersion+"_"+date+"_"+country+".csv";
				filepath=Constants.RESULT_DATA+analysisVersion+"/"+analysisVersion+"_"+date+"_"+tableID;
				if(probeDataSource==SOURCE.QML) filepath+="_qml";
				filepath+=".csv";
				System.out.println("Load result file "+filepath);
				
				v4ReadStatResult(filepath, allPairs, tmcs, analysisVersion, date,country, "HALO", directedExtractedFromGroundtruthTable);
			}	
			
			//sort conditions in alphabetical order
			ArrayList<String> conditions=new ArrayList<String>(allPairs.keySet());
			Collections.sort(conditions, new Comparator<String>(){
				@Override
				public int compare(String s1, String s2) {
					int lenDiff=s1.length()-s2.length();
					if(lenDiff==0){
						return s1.compareTo(s2);
					}
					return lenDiff;
				}
			});
			
			/**
			 * variable for 3D Scatter
			 */
			ArrayList<ArrayList<Double>> xSeries=new ArrayList<ArrayList<Double>>();
			ArrayList<ArrayList<Double>> ySeries=new ArrayList<ArrayList<Double>>();
			ArrayList<ArrayList<Double>> zSeries=new ArrayList<ArrayList<Double>>();
			
			try{
				String outputFilePath=Constants.RESULT_DATA+analysisVersion+"/"+analysisVersion;
				outputFilePath+="_final";
				if(!trafficConditionDichotomy) outputFilePath+="_speed";
				if(laneBased) outputFilePath+="_laneBased";
				if(carpoolExpressLane) outputFilePath+="_specialLane";
				if(tableID.length()>0) outputFilePath+="_"+tableID;
				if(probeDataSource==SOURCE.QML) outputFilePath+="_qml";
				if(minNOPairsInOneBucket!=200) outputFilePath+="_bucketsize="+minNOPairsInOneBucket;
				outputFilePath+=".txt";
				
				FileWriter fw=new FileWriter(outputFilePath);
				for(String trafCond:conditions){
					System.out.println(title+ " "+trafCond);
					
			
					ArrayList<EpochTMC> epochTMCs=allPairs.get(trafCond);
					
					//skip over conditions with zero pairs
					if(epochTMCs.size()==0) continue;
					
					ArrayList<DensityBucket> buckets=new ArrayList<DensityBucket>();
					buckets.add(new DensityBucket());//first bucket
					
					//initialize hardcoded bins
					int noOfBins=1000;int binStep=1;
					double[] hardCodedBins=new double[noOfBins];
					for(int i=1;i<noOfBins;i++) hardCodedBins[i]=hardCodedBins[i-1]+binStep;
					//System.out.println(Arrays.toString(hardCodedBins));
					
					/***
					 * sort epochTMC based on # of probes
					 */
					for(EpochTMC epochTMC:epochTMCs){
						//TODO define the field to be ranked
						//epochTMC.setToBeSortedField(epochTMC.noOfProbes);
						epochTMC.setToBeSortedField(epochTMC.noOfProbesPerMile);
					}				
					Collections.sort(epochTMCs, new Comparator<EpochTMC>() 
					{
					    public int compare(EpochTMC o1, EpochTMC o2) 
					    {
					       if(o1 instanceof EpochTMC && o2 instanceof EpochTMC) 
					       {
					    	   double diff=((EpochTMC)o1).fieldToBeSorted-((EpochTMC)o2).fieldToBeSorted;
					    	   if(diff>0) return 1;
					    	   else{
					    		   if(diff<0)return -1;
					    		   else return 0;
					    	   }
					       } 
					       return 0;    
					    }
					});	
					
					/**
					 * Bucketise the probe density
					 */
					int nextHardCodedBinIdx=0;
					double lastValue=-1;
					int cnt=0;
					int binIdx=1;
					DensityBucket lastBucket=buckets.get(buckets.size()-1);
					
					for(int pairIdx=0;pairIdx<epochTMCs.size();pairIdx++){					
						EpochTMC epochTMC=epochTMCs.get(pairIdx);
						if(tableID.length()>0){
							if(!epochTMC.tmc.substring(0,3).equals(tableID))continue;
						}
						
						if(lastBucket.pairs.size()>=minNOPairsInOneBucket
				&&nextHardCodedBinIdx<hardCodedBins.length&&epochTMC.fieldToBeSorted>hardCodedBins[nextHardCodedBinIdx]
						&&epochTMC.fieldToBeSorted>lastValue){
							
							DensityBucket dBucket=new DensityBucket();
							dBucket.lowerBound=epochTMC.fieldToBeSorted;
							if(binIdx>=1){//not first bucket, update the upper bound of previous bucket 
								buckets.get(buckets.size()-1).upperBound=dBucket.lowerBound;
							}
							
							buckets.add(dBucket);
							binIdx++;
							nextHardCodedBinIdx++;
						}
						lastValue=epochTMC.fieldToBeSorted;
								
						cnt++;
						lastBucket=buckets.get(buckets.size()-1);
						lastBucket.pairs.add(epochTMC);
						//lastBucket.providerSetSize+=epochTMC.providerSetSize;
						/*if(!isCongestion&&avgProbeCnt[b]>30){
							System.out.println("density :" + avgProbeCnt[i]);
							System.out.println(valuesWithinBin.get(i));
						}*/
					}			

					
					//System.out.println("densityIdx="+binIdx);
					double sum=0, sum1=0;
				
					/**
					 * calculate stats for each bucket
					 */
					for(int i=0;i<binIdx;i++){
						DensityBucket dBucket=buckets.get(i);
						
						int n=dBucket.pairs.size();
						sum1+=n;
						
						HashSet<Integer> population=null;
						if(samePopulationForBuckets)population=CommonUtils.randomByShuffle(minNOPairsInOneBucket, n);
						
						double[] errors=new double[n];
						double[] groundTruthSpeed=new double[n];
						
						for(int pairIdx=0;pairIdx<n;pairIdx++){
							if(samePopulationForBuckets&&!population.contains(pairIdx)) continue;
							EpochTMC epochTMC=dBucket.pairs.get(pairIdx);
							
							dBucket.avgProbeCnt+=epochTMC.noOfProbes;
							dBucket.avgProbeCntPerMile+=epochTMC.noOfProbesPerMile;
							dBucket.avgProbSpdStd+=epochTMC.probeSpeedStd;
							dBucket.avgProbeSpdMean+=epochTMC.probeSpeedMean;
							
							errors[dBucket.pairCnt]=epochTMC.error;
							groundTruthSpeed[dBucket.pairCnt]=epochTMC.groundTruthSpeed;
							
							/*if(i==1&&pairIdx<10){
								System.out.println(epochTMC);
							}*/
						
							if(Math.abs(epochTMC.error)>10){
								/*if(i==binIdx-1){
									System.out.println(epochTMC);
								}*/
								
								dBucket.cntOfPairFallOutBand+=1.0;
							}
							dBucket.pairCnt+=1;
						}
						
						
						
						dBucket.avgProbeCnt/=dBucket.pairCnt;
						dBucket.avgProbeCntPerMile/=dBucket.pairCnt;
						dBucket.avgProbeSpdMean/=dBucket.pairCnt;
						dBucket.avgProbSpdStd/=dBucket.pairCnt;
						
						dBucket.avgQualityScore=(1.0-dBucket.cntOfPairFallOutBand/dBucket.pairCnt)*100;
									
						errors=Arrays.copyOf(errors, dBucket.pairCnt);
						dBucket.avgError=mean.evaluate(errors);
						dBucket.stdError=Math.sqrt(var.evaluate(errors, dBucket.avgError) );
						
						dBucket.groundTruthSpeed=Arrays.copyOf(groundTruthSpeed, dBucket.pairCnt);
						//get the average groundtruth speed
						dBucket.avgGroundTruthSpeed=mean.evaluate(groundTruthSpeed);
						
						//plot
						//if(isCongestion)
						if(plotting){
							if(i<3||binIdx-i<=3){//plot the distribution
								String cond=trafCond;
								String figTitle=cond+"_"+i+" th bin: "+dBucket.lowerBound;
								if(i>0){
									figTitle+=" ~ "+dBucket.upperBound;
								}
								//plot the histogram of errors
								//Plot.histogram(figTitle, errors, 20, Constants.V5_RES_DATA+"figs/errors/"+cond+"_"+i+".jpg");
								//Plot.histogram(figTitle, groundTruthSpeed, 8, Constants.V5_RES_DATA+"figs/gt_speed/"+cond+"_"+i+".jpg");
							}
						}
						
						sum+=dBucket.pairCnt;
					}
					
					
					/**
					 * Output to files to be plotted by python
					 */
					fw.write("---"+trafCond+"\n");
					System.out.println("pariCnt="+cnt+" sumOfPairCntOfBuckets="+sum+" sum1="+sum1);
					System.out.println("densityRange,#ofPairs,avgProbeSpdStd,avgProbeSpdMean,avgDensity,avgError,avgStdError,avgQS,avgGroundTruthSpd");
					String msg;
					for(int i=0;i<binIdx;i++){
						msg="";
						DensityBucket dBucket=buckets.get(i);
						if(i>0) msg+="["+String.format("%5.2f", dBucket.lowerBound)+"~";
						else msg+="[ 0.00~";
						if(i<binIdx-1) msg+=String.format("%5.2f",dBucket.upperBound)+"),";
						else msg+="     ],";
						msg+=String.format("%7d", dBucket.pairCnt)+",";
						msg+=//String.format("%.2f", avgProbeCnt[i])
							String.format("%13.2f", dBucket.avgProbSpdStd)+","+String.format("%13.2f", dBucket.avgProbeSpdMean)
								
							+","+String.format("%10.2f", dBucket.avgProbeCntPerMile)
							+","+String.format("%10.2f", dBucket.avgError)+","+String.format("%10.2f", dBucket.stdError)
							+","+String.format("%10.2f",dBucket.avgQualityScore)+"%"
							+","+String.format("%10.2f", dBucket.avgGroundTruthSpeed);
						//System.out.print(dBucket.providerSetSize+" ");
						System.out.println(msg);
						fw.write(msg+"\n");
					}
					
					/**
					 * Calculate Pearson correlation
					 */
					double[] avgProbeCnt=new double[buckets.size()];
					double[] avgProbeCntPerMile=new double[buckets.size()];
					double[] avgErrors=new double[buckets.size()];
					double[] avgQualityScore=new double[buckets.size()];
					
					if(buckets.size()>0){
						xSeries.add(new ArrayList<Double>());
						ySeries.add(new ArrayList<Double>());
						zSeries.add(new ArrayList<Double>());
					}
					
					for(int i=0;i<buckets.size();i++){
						DensityBucket dBucket=buckets.get(i);
						avgProbeCnt[i]=dBucket.avgProbeCnt;
						avgProbeCntPerMile[i]=dBucket.avgProbeCntPerMile;
						avgErrors[i]=Math.abs(dBucket.avgError);
						avgQualityScore[i]=dBucket.avgQualityScore;
						
						if(buckets.size()>0){
							if(trafficConditionDichotomy){
								if(trafCond.toLowerCase().contains("free")) xSeries.get(xSeries.size()-1).add(2.0);
								else xSeries.get(xSeries.size()-1).add(1.0);
							}else{
								xSeries.get(xSeries.size()-1).add(mean.evaluate(dBucket.groundTruthSpeed));
								//xSeries.get(xSeries.size()-1).add(Double.parseDouble(trafCond));
							}
							ySeries.get(ySeries.size()-1).add(avgProbeCntPerMile[i]);
							
							zSeries.get(zSeries.size()-1).add(avgQualityScore[i]); //quality score
							//zSeries.get(zSeries.size()-1).add(avgErrors[i]); //avg error
						}				
					}
					
					
					PearsonsCorrelation pc=new PearsonsCorrelation();
					System.out.println("correlation between avgProbeCnt and avgError is "+String.format("%.2f", pc.correlation(avgProbeCnt, avgErrors)));
					System.out.println("correlation between avgProbeCnt and avgQualityScore is "+String.format("%.2f", pc.correlation(avgProbeCnt, avgQualityScore)));
					System.out.println("correlation between avgProbeCntPerMile and avgError is "+String.format("%.2f", pc.correlation(avgProbeCntPerMile, avgErrors)));
					System.out.println("correlation between avgProbeCntPerMile and avgQualityScore is "+String.format("%.2f", pc.correlation(avgProbeCntPerMile, avgQualityScore)));
					System.out.println();
						
				}
				fw.close();
				
			}catch(Exception ex){
				ex.printStackTrace();
			}
			
			/**
			 * Plot the 3D scatter
			 */	
			if(plotting){
				Plot.scatter3D("Traffic Condition", xSeries, ySeries, zSeries);
			}
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
					
					Double groundTruthSpeed=Double.parseDouble(fields[27]);
					EpochTMC epochTMC=new EpochTMC(date, tmc, epochIdx, covered, error, engine+"-"+flowType+"-"+isHyw, groundTruthSpeed);
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
		analysisVersion="v2";

		HashMap<String, TMC> tmcs=TMC.loadTMC("A0");
		
		String[] dates={"20131213","20131212","20131220"};
		
		for(String date:dates){
			HashMap<String, MarketV2> markets=v2ReadGroundTruth(date);
			//for(Market market: markets.values()) System.out.println(market);		
			HashMap<String, String> tmcToMarket=loadTMCToMarket("A0", markets, tmcs); 

			
			File baseFolder=new File(Constants.RESULT_DATA+analysisVersion+"/"+analysisVersion+"_"+date+"/");
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
					XAxisMetric metric=market.densityMetrics;
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
						XAxisMetric metric=market.densityMetrics;
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
	
	public HashMap<String, MarketV2> v2ReadGroundTruth(String date){
		HashMap<String, MarketV2> markets=new HashMap<String, MarketV2>();
		try{
			Scanner sc=new Scanner(new File(Constants.RESULT_DATA+analysisVersion+"/"+date+"/ground_truth_"+date+".txt"));
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
				ConditionV2 condition=new ConditionV2(engine, timePeriod, "ALL");
				market.qualityMetrics.put(condition.toString(), new YAxisMetric(Double.parseDouble(fields[Constants.IDX_ROAD_CONDITION_ALL])));
				ConditionV2.allConditonV2s.add(condition.toString());
				
				condition=new ConditionV2(engine, timePeriod, "Congestion");
				market.qualityMetrics.put(condition.toString(), new YAxisMetric(Double.parseDouble(fields[Constants.IDX_ROAD_CONDITION_CONGESTION])));
				ConditionV2.allConditonV2s.add(condition.toString());
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
			File areaTMCToMarket=new File(Constants.TMC_DATA+extendCountryCode+"_tmcToMarket.txt");
			if(areaTMCToMarket.exists()){
				br = new BufferedReader(new FileReader(areaTMCToMarket));
				
			}else{
				br = new BufferedReader(new FileReader(Constants.TMC_DATA+"tmcToMarket.txt"));
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
				vehicleCnts.get(tmc).add(fields[Constants.RAW_PROBE_IDX_VEHICLE_ID]);
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
