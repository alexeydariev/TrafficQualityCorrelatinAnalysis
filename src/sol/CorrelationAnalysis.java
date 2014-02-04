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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.commons.math3.stat.descriptive.moment.Mean;


public class CorrelationAnalysis {
	public static void main(String[] args) {
		new CorrelationAnalysis().main();
	}
	
	public static String analysisVersion;
	
	public void main(){
		
		secondAttempt();
		//thirdAttempt();
		//mergeResultFiles("v3");
	}
	
	public void thirdAttempt(){
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
					double error=Double.parseDouble(fields[29]);
					
					//TODO the source data seems suspicious
					//if(error>400) System.out.println(line);
					
					String engine=fields[11];
					String isHyw=fields[12];
					String flowType=fields[36];
					
					EpochTMC epochTMC=new EpochTMC(tmc, epochIdx, covered, error, engine+"-"+flowType+"-"+isHyw);
					if(!epochTMCPairsIndexedByEpoch.containsKey(epochIdx)) epochTMCPairsIndexedByEpoch.put(epochIdx, new ArrayList<EpochTMC>());
					epochTMCPairsIndexedByEpoch.get(epochIdx).add(epochTMC);
				}
				System.out.println("# of epoches in the ground truth ="+epochTMCPairsIndexedByEpoch.size());
				//for(int epoch: epochTMCPairsIndexedByEpoch.keySet()) System.out.println(epochTMCPairsIndexedByEpoch.get(epoch).size()+"  ");
				br.close();
				
				/**
				 * Calculate the stats
				 */
				HashMap<String, String> tmcToMarket=loadTMCToMarket("A0", null); 
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
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public void secondAttempt(){
		analysisVersion="v2_";

		HashMap<String, TMC> tmcs=loadTMC("A0");
		
		String[] dates={"20131213","20131212","20131220"};
		
		for(String date:dates){
			HashMap<String, MarketV2> markets=readGroundTruth(date);
			//for(Market market: markets.values()) System.out.println(market);		
			HashMap<String, String> tmcToMarket=loadTMCToMarket("A0", markets); 

			
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
				readStatProbeFile(tmcs,markets, tmcToMarket, statFile.getAbsolutePath());
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
	
	public void readProbeFileOutputStats(String fileName){
		HashMap<String, Integer> probeCnts=new HashMap<String, Integer>();
		HashMap<String, HashSet<String>> vehicleCnts=new HashMap<String, HashSet<String>>();
		HashMap<String, HashSet<String>> providerCnts=new HashMap<String, HashSet<String>>();
		String line="";
		try{
			//analyze 
			String filePath=Constants.PROBE_RAW_DATA+fileName;
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
				
				//a new tmc
				if(!probeCnts.containsKey(tmc)){
					probeCnts.put(tmc, 1);
					vehicleCnts.put(tmc, new HashSet<String>());
					providerCnts.put(tmc, new HashSet<String>());
				}else{
					int oldCnt=probeCnts.get(tmc);
					probeCnts.remove(tmc);
					probeCnts.put(tmc, oldCnt+1);
				}
				vehicleCnts.get(tmc).add(fields[Constants.IDX_PROBE_ID]);
				providerCnts.get(tmc).add(fields[Constants.IDX_VENDOR_DESC]);
			}
			br.close();
			
			//output the stats to a file
			FileWriter fw=new FileWriter(Constants.PROBE_RAW_DATA+"STAT_"+fileName);
			for(String tmc: probeCnts.keySet()){
				fw.write(tmc+","+probeCnts.get(tmc)+","+vehicleCnts.get(tmc).size()+","+providerCnts.get(tmc).size()+
			"\n");
			}
			fw.close();
			System.out.println(cnt);
		}catch(Exception ex){
			System.out.println("line ="+line);
			ex.printStackTrace();
		}
	}
	
	public void readStatProbeFile(HashMap<String,TMC> tmcs, HashMap<String, MarketV2> markets,HashMap<String,String> tmcToMarket,String filepath){
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
	
	public HashMap<String, String> loadTMCToMarket(String extendCountryCode, HashMap<String, MarketV2> markets){
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
				if(markets!=null&&markets.containsKey(market)) markets.get(market).tmcs.add(tmc);				
			}
			br.close();
			if(fw!=null) fw.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return tmcToMarket;
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
