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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;


public class CorrelationAnalysis {
	public static void main(String[] args) {
		new CorrelationAnalysis().main();
	}
	
	public void main(){
		String date="20131212";
		HashMap<String, Market> tmcToMarket=new HashMap<String, Market>();
		HashMap<String, Market> markets=readGroundTruth(tmcToMarket, date);
		

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
				Market market=markets.get(name);
				XAXisMetric metric=market.densityMetrics;
				metric.avgProbeCntPerTMC=metric.sumOfProbes/market.tmcs.size();
				metric.avgVehicleCntPerTMC=metric.sumOfVehicles/market.tmcs.size();
				fw.write(market+"\n");
			}
			fw.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		
		//readProbeFileOutputStats("20131212_6,7,8_probe.csv");
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
	
	public void readStatProbeFile(HashMap<String, Market> markets,HashMap<String,Market> tmcToMarket,String filepath){
		String tmc=null;
		try{
			Scanner sc=new Scanner(new File(filepath));
			while(sc.hasNextLine()){
				String[] fields=sc.nextLine().split(",");
				if(fields.length!=4) continue;
				//System.out.println(Arrays.toString(fields));
				tmc=fields[0].trim();
				
				if(tmcToMarket.containsKey(tmc)){
					Market market=tmcToMarket.get(tmc);
					XAXisMetric metric=market.densityMetrics;
					metric.sumOfProbes+=Integer.parseInt(fields[1]);
					metric.sumOfVehicles+=Integer.parseInt(fields[2]);
					metric.sumOfProviders+=Integer.parseInt(fields[3]);
				}
			}
		}catch(Exception ex){
			System.out.println("tmc="+tmc);
			ex.printStackTrace();
		}
	}
	
	public HashMap<String, Market> readGroundTruth(HashMap<String, Market> tmcToMarket, String date){
		HashMap<String, Market> markets=new HashMap<String, Market>();
		try{
			Scanner sc=new Scanner(new File(Constants.PROBE_STAT_DATA+date+"/ground_truth_"+date+".txt"));
			while(sc.hasNextLine()){
				String[] fields=sc.nextLine().split(",");
				if(!fields[fields.length-1].equals("ALL DAY")) continue;
				
				String engine=fields[6].trim();
				
				String marketName=fields[0].replace("\\", "");
				Market market;
				if(!markets.containsKey(marketName)){
					market=new Market(marketName);
					markets.put(marketName, market);
				}else{
					market=markets.get(marketName);
				}
				String tmc=fields[3].trim();
				market.tmcs.add(tmc);
				market.qualityMetrics.get(engine).qualityScore=Double.parseDouble(fields[2]);
				
				//System.out.println(market.toString());
				if(!tmcToMarket.containsKey(tmc)){
					tmcToMarket.put(tmc, market);
				}
			}
			System.out.println("# of markets="+markets.size()+"\n# of total tmc's="+tmcToMarket.size());
			/*for(String market: markets.keySet()){
				System.out.println(market+"  "+markets.get(market).tmcs.size());
			}*/
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return markets;
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
