package sol;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;


public class CorrelationAnalysis {
	public static void main(String[] args) {
		new CorrelationAnalysis().main();
	}
	
	public void main(){
		secondAttempt();
		thirdAttempt();
		//readProbeFileOutputStats("20131212_6,7,8_probe.csv");
	}
	
	public void thirdAttempt(){
		try{
			HashMap<String, String> tmcToMarket=loadTMCToMarket("A0", null); 
			
			
			HashMap<String, MarketV3> markets=new HashMap<String, MarketV3>();
			HashMap<Integer, ArrayList<EpochTMC>> epochTMCPairsIndexedByEpoch=new HashMap<Integer, ArrayList<EpochTMC>>();
			//build up epochs
			String date="20131220";
			BufferedReader br = new BufferedReader(new FileReader(Constants.GROUND_TRUTH_DATA+date));
			String line;
			while((line=br.readLine())!=null){
				
			}
			br.close();
			
			/**
			 * Calculate the stats
			 */
			for(int epochIdx=0;epochIdx<24*60/EpochTMC.EPOCH_DURATION;epochIdx++){
				//for each epoch
				for(MarketV3 market: markets.values()){
					market.noOfCoveredTMCsInOneEpoch=0;
					market.noOfTMCsInOneEpoch=0;
					market.errorInOneEpoch=0;
				}
				for(EpochTMC pair: epochTMCPairsIndexedByEpoch.get(epochIdx)){
					MarketV3 market= markets.get( tmcToMarket.get(pair.tmc) );
					
					market.noOfTMCsInOneEpoch+=1;
					if(pair.covered) market.noOfCoveredTMCsInOneEpoch+=1;
					market.errorInOneEpoch+=pair.error;
				}
				for(MarketV3 market: markets.values()){
					market.coverages.add((market.noOfCoveredTMCsInOneEpoch+0.0)/market.noOfTMCsInOneEpoch);
					market.errors.add(market.errorInOneEpoch/market.noOfTMCsInOneEpoch);
				}
			}
			
			
		}catch(Exception ex){
			
		}
	}
	
	public void secondAttempt(){
		String date="20131213";
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
			readStatProbeFile(markets, tmcToMarket, statFile.getAbsolutePath());
		}
		try{

			FileWriter fw=new FileWriter(Constants.PROJECT_FOLDER+"bin/"+date+".csv");
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
	
	public void readStatProbeFile(HashMap<String, MarketV2> markets,HashMap<String,String> tmcToMarket,String filepath){
		String tmc=null;
		try{
			Scanner sc=new Scanner(new File(filepath));
			while(sc.hasNextLine()){
				String[] fields=sc.nextLine().split(",");
				if(fields.length!=4) continue;
				//System.out.println(Arrays.toString(fields));
				tmc=fields[0].trim();
				
				if(tmcToMarket.containsKey(tmc)){
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
					market=new MarketV2(marketName);
					markets.put(marketName, market);
				}else{
					market=markets.get(marketName);
				}
				
				String engine=fields[Constants.IDX_ENGINE_TYPE].trim(), timePeriod=fields[Constants.IDX_TIME_PERIOD];
				Conditon condition=new Conditon(engine, timePeriod, "ALL");
				market.qualityMetrics.put(condition, new YAxisMetric(Double.parseDouble(fields[Constants.IDX_ROAD_CONDITION_ALL])));
				condition=new Conditon(engine, timePeriod, "Congestion");
				market.qualityMetrics.put(condition, new YAxisMetric(Double.parseDouble(fields[Constants.IDX_ROAD_CONDITION_CONGESTION])));
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
