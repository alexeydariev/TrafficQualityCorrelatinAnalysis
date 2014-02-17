package sol;

public final class Constants {
	/**
	 * Directory setting
	 */
	public static final String PROJECT_FOLDER="E:/workspace/java/TrafficQualityCorrelatinAnalysis/";
	
	public static final String BIN_FOLDER=PROJECT_FOLDER+"bin/";
	public static final String DATA_FOLDER=PROJECT_FOLDER+"data/";
	public static final String PROBE_RAW_DATA=DATA_FOLDER+"raw/";
	public static final String PROBE_STAT_DATA=DATA_FOLDER+"stat/";
	public static final String RESULT_DATA=DATA_FOLDER+"res/";
	public static final String MAP_DATA=DATA_FOLDER+"map/";
	public static final String GROUND_TRUTH_DATA=DATA_FOLDER+"ground_truth/";
	
	
	/**
	 * Ground truh data field index
	 */
	public static final int IDX_CONTINENT=0;
	public static final int IDX_COUNTRY=1;
	public static final int IDX_MARKET=2;
	public static final int IDX_ENGINE_TYPE=4;
	public static final int IDX_DATE=5;
	public static final int IDX_TIME_PERIOD=6;
	public static final int IDX_ROAD_CONDITION_ALL=7;
	public static final int IDX_ROAD_CONDITION_CONGESTION=8;
	
	/**
	 * Threshold
	 */
	public static final int PROBE_CNT_THRSHOLD=10;
	public static final int VEHICLE_CNT_THRESHOLD=3;

	/**
	 * Raw probe data field index
	 */
	public static final int IDX_SAMPLE_DATE=0;
	public static final int IDX_SYS_DATE=1;
	public static final int IDX_VENDOR_DESC=2;
	public static final int IDX_PROBE_ID=3;
	public static final int IDX_COOR_Y=4;
	public static final int IDX_COOR_X=5;
	public static final int IDX_HEADING=6;
	public static final int IDX_SPEED=7;
	public static final int IDX_CTY_CODE=8;
	public static final int IDX_TABLE_ID=9;
	public static final int IDX_TMC_DIR=10;
	public static final int IDX_TMC_POINT_LOC_CODE=11;
	
	
	
	/***
	 * Netezza database
	 */
		/**
		 * accounts info.
		 */
		public static final String NETEZZA_SOCKET="10.228.211.80:5480";
		public static final String NETEZZA_DBNAME="Sparky";
		public static final String NETEZZA_ACCOUNT="cca_user";
		public static final String NETEZZA_PASSWORD="password";
		
		/**
		 * Table Name
		 */
		public static final String MAP_MATCHED_PROBE_DATA="mm" ;
		public static final String GROUND_TRUTH_DRIVING="";
		public static final String GROUND_TRUTH_PATH="";
		public static final String GROUND_TRUTH_SENSOR="";
	
	/**
	 * Oracle database
	 */
		/**
		 * accounts info.
		 */
		public static final String ORACLE_SOCKET="ploratrfdw01-v.traffic.com:1523";
		public static final String ORACLE_DBNAME="PRBWP.traffic.com";
		public static final String ORACLE_ACCOUNT="read_user";
		public static final String ORACLE_PASSWORD="read_user";
	
		
		
	
	
}
