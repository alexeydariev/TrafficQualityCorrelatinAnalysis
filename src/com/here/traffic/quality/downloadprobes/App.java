package com.here.traffic.quality.downloadprobes;

import java.io.*;

import com.here.traffic.quality.LogWriter;
import com.here.traffic.quality.SqlQuery;
import com.here.traffic.quality.correlation.Constants;
import com.here.traffic.quality.correlation.CorrelationAnalysis;
import com.javacodegeeks.java.core.CompressFileGzip;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;

import javax.xml.bind.JAXBContext;

import java.util.zip.GZIPOutputStream;

/**
 * Hello world!
 *
 */
final public class App implements AutoCloseable {

    private Connection prbwp, nz;
    private static LogWriter LOG = new LogWriter();
    private String path = null;
    private SqlQuery sql = new SqlQuery();
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    private Date startDate, endDate;
    static Properties properties;
    static String properties_filename = "download.properties";
    public String m_CC, m_Table, m_Date,m_Suffix,m_NonTmcOnly;
    private JAXBContext jaxbContext;
    
    public CorrelationAnalysis analysis;

    App() throws Exception {

        //identify folder path external to jar file
        //this.path = App.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        //this.path = this.path.substring(0, this.path.lastIndexOf("/") + 1);
    	this.path=Constants.PROBE_RAW_DATA;
    	
        //analysis instance
        analysis=new CorrelationAnalysis();
        
        //code to randomize order of PRBWP servers
        HashSet<Integer> numbers = new HashSet<>();
        numbers.add(1);
        numbers.add(2);
        numbers.add(3);
        Integer[] servers = new Integer[3];
        servers[0] = Math.round(1 + Math.round(Math.random() * 2));
        numbers.remove(servers[0]);

        do {
            servers[1] = Math.round(1 + Math.round(Math.random() * 2));
        } while (servers[1] == servers[0]);
        numbers.remove(servers[1]);

        for (Integer i : numbers) {
            servers[2] = i;
        }

        //initialize Oracle/PRBWP connection
        Class.forName("oracle.jdbc.driver.OracleDriver");
        this.prbwp = DriverManager.getConnection("jdbc:oracle:thin:@(DESCRIPTION = "
                + "(ADDRESS = (PROTOCOL = TCP)(HOST = ploratrfdw0" + servers[0] + "-v.traffic.com)(PORT = 1523)) "
                + "(ADDRESS = (PROTOCOL = TCP)(HOST = ploratrfdw0" + servers[1] + "-v.traffic.com)(PORT = 1523)) "
                + "(ADDRESS = (PROTOCOL = TCP)(HOST = ploratrfdw0" + servers[2] + "-v.traffic.com)(PORT = 1523)) "
                + "(FAILOVER = ON) (LOAD_BALANCE = YES) (CONNECT_DATA = (SERVER = DEDICATED) "
                + "(SERVICE_NAME = PRBWP.traffic.com) (FAILOVER_MODE = (TYPE = SELECT)(METHOD = BASIC) (RETRIES = 20)(DELAY = 5))))", "read_user", "read_user");

        //initialize Netezza connection
        /*
         Class.forName("org.netezza.Driver");
         this.nz = DriverManager.getConnection("jdbc:netezza://PEQXNTZA:5480/QA_SAM","QA_SAM_USER","password");
         */
    }

    public static void main(String[] args) {
        /*if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                String l = a.toLowerCase();
                if (l.endsWith(".properties")) {
                    properties_filename = a;
                }
            }
        }*/
        
    	//read parameters from property file
    	/*properties_filename=System.getProperty("user.dir")+"/src/main/resources/download.properties";
        properties = new Properties();
    	try {
            //File ff = new File(properties_filename);
            InputStreamReader isrProp = new InputStreamReader(new FileInputStream(properties_filename));
            properties.load(isrProp);
            isrProp = new InputStreamReader(new FileInputStream(properties_filename));
            properties.load(isrProp);
        } catch (Exception e) {
            e.printStackTrace();
        }*/

        try (App app = new App()) {
            System.out.println("path="+app.path);
        	//raw probe data will be loaded daily, but corroboration will only be done on Mondays
            
            app.downloadData();
            LOG.closeLog();
            System.exit(0);
        } catch (Exception e) {
            LOG.logTrace(e);
            LOG.closeLog();
            e.printStackTrace();
            System.exit(1);
        }

    }

    
    private void downloadData() {
        m_CC = "1"; //D for 'Germany', F for 'France', 1 for 'US'
        //m_Table = "1,2";
        m_Date = "20140213";
        m_Suffix = "_probe";
        m_NonTmcOnly = "0";

        int noOfBatches=11, noOfTablePerBatch=3;
        for(int i=0;i<noOfBatches;i++){
        	
        	//m_Table="32";
        	m_Table="";
        	for(int j=1;j<=noOfTablePerBatch;j++){
        		if(j>1) m_Table+=",";
        		m_Table+=noOfTablePerBatch*i+j;
        	}
        	//System.out.println(m_Table);
        	
        	SimpleDateFormat formatter;
            formatter = new SimpleDateFormat("yyyyMMdd");

            /*Date date = new Date(new Date().getTime() - 86400000);*/

            String[] ary1 = m_Date.split(",");
            int x = 0;
            int dateArrayLength = ary1.length;
            //System.out.println(dateArrayLength);
            
            
            Date date = null;
            try {
                date = formatter.parse(ary1[0]);//user entry date
            } catch (Exception ignore) {
            }
            
            /**
             * Probe raw data download
             */
            String filedate = sdf.format(date);
            String filename = path + filedate + "_"+m_Table + m_Suffix + ".csv";
            File file = new File(filename);
            
            
            //if (file.isFile()){ file.delete(); }
            //file.deleteOnExit();

//            sql.addExtractQuery(prbwp, file, "SELECT TO_CHAR(SAMPLE_DATE,'YYYY-MM-DD HH24:MI:SS') || ',' || PROBE_DATA_PROVIDER_DESC || ',' || PROBE_ID || ',' || " + 
//                "EBU_COUNTRY_CODE || LPAD(TO_CHAR(LOCATION_TABLE_NR),2,'0') || ',' || DECODE(TMC_PATH_DIRECTION,'+','P','-','N',TMC_PATH_DIRECTION) || ',' || POINT_LOCATION_CODE || ',' || " + 
//                "ROUND(P.GEOLOC.SDO_POINT.Y,5) || ',' || ROUND(P.GEOLOC.SDO_POINT.X,5) || ',' || ROUND(SPEED) " + 
//                "FROM PROBE.PROBE_DATA_RAW_ARCHIVE PARTITION(P" + sdf.format(date) + ") P WHERE (EBU_COUNTRY_CODE = 'D' AND LOCATION_TABLE_NR IN (1)) AND MAPPED_FLAG = 'Y' AND ROUND(SPEED) BETWEEN 1 AND 149 " + 
//                "AND (SYSTEM_DATE-SAMPLE_DATE)*1440 BETWEEN 0 AND 25");

            int y = 0;
            
            while (x < dateArrayLength)    
            {
            	System.out.println(".....Start Downloading "+file.getName()+" ......................");
                y = x+1;
                if (m_NonTmcOnly.equals("1"))    
                {
                    try 
                    {
                        date = formatter.parse(ary1[x]);//user entry date
                    }
                    catch (Exception ignore)
                    {
                    }
//                    System.out.println(x);
//                    System.out.println(sdf.format(date));

                    sql.addExtractQuery(prbwp, file, "SELECT PROBE_ID || ',' || TO_CHAR (SAMPLE_DATE, 'YYYY-MM-DD HH24:MI:SS') || ',' || P.GEOLOC.SDO_POINT.Y || ',' || "
                        + "P.GEOLOC.SDO_POINT.X || ',' || HEADING || ',' || SPEED || ',' || HEADING || ',' || SPEED || ',' || PROBE_DATA_PROVIDER_DESC "
                        + "FROM PROBE.PROBE_DATA_RAW_ARCHIVE PARTITION(P" + sdf.format(date) + ") P WHERE POINT_LOCATION_CODE IS NULL AND (EBU_COUNTRY_CODE IN ('" + m_CC + "') AND LOCATION_TABLE_NR IN (" + m_Table + "))");        
                    
                    if (sql.executeExtractLoad()) 
                    {
                        LOG.writeLog("Probe data " + y + "/" + dateArrayLength + " loaded successfully.");
                    }
                    else
                    {
                        LOG.writeLog("Problem with probe-data load. Process failed.");
                        System.exit(1);
                    }
                }

                else
                {
                    sql.addExtractQuery(prbwp, file, "SELECT TO_CHAR (SAMPLE_DATE, 'YYYY-MM-DD HH24:MI:SS') || ',' || TO_CHAR (SYSTEM_DATE, 'YYYY-MM-DD HH24:MI:SS') || ',' || PROBE_DATA_PROVIDER_DESC || ',' || "
                        + "PROBE_ID || ',' || P.GEOLOC.SDO_POINT.Y  || ',' || P.GEOLOC.SDO_POINT.X || ',' || HEADING || ',' || SPEED || ',' || EBU_COUNTRY_CODE || ',' || "
                        + "LPAD (LOCATION_TABLE_NR, 2, '0') || ',' || TMC_PATH_DIRECTION || ',' || POINT_LOCATION_CODE "
                        + "FROM PROBE.PROBE_DATA_RAW_ARCHIVE PARTITION(P" + sdf.format(date) + ") P WHERE (EBU_COUNTRY_CODE IN ('" + m_CC + "') )"
                        + "AND MAPPED_FLAG='Y'"
                        + "AND LOCATION_TABLE_NR IN (" + m_Table + ")"
                    );
                    
                    if (sql.executeExtractLoad()) 
                    {
                        LOG.writeLog("Probe data " + y + "/" + dateArrayLength + " loaded successfully.");
                    }
                    else
                    {
                        LOG.writeLog("Problem with probe-data load. Process failed.");
                        System.exit(1);
                    }
                }
                x=x+1;
            }
            System.out.println("....."+file.getName()+" is downloaded....");

            /*
             sql.addLoadQuery(nz, "insert into PROBE_DATA_RAW_STAGE select * from EXTERNAL '" + file.getAbsolutePath() + "' USING ( Format 'TEXT' Compress FALSE RemoteSource 'JDBC' SocketBufSize 67108864 Encoding 'INTERNAL' LogDir '" + path + "' MaxRows 0 SkipRows 0 MaxErrors 1000 FillRecord true EscapeChar '' CrInString false CtrlChars false IgnoreZero false TimeExtraZeros false Y2Base 0 TruncString false AdjustDistZeroInt false Delimiter ',' NullValue '' QuotedValue 'DOUBLE' RequireQuotes false DateStyle 'YMD' DateDelim '-' TimeStyle '24HOUR' TimeDelim ':' BoolStyle '1_0' DecimalDelim '.' DisableNfc false )");
             sql.addLoadQuery(nz, "INSERT INTO PROBE_DATA_RAW SELECT DISTINCT SAMPLE_DATE, PROBE_DATA_PROVIDER_DESC, PROBE_ID, TABLE_NBR, DIRECTION, LOCATION_CODE, LATITUDE, LONGITUDE, SPEED FROM PROBE_DATA_RAW_STAGE");
             sql.addLoadQuery(nz, "TRUNCATE TABLE PROBE_DATA_RAW_STAGE");
             */


            //compress the raw data file
            /*String destinaton_zip_filepath = path + "ZIP_" + filedate + m_Suffix + ".csv.gz";
            CompressFileGzip gZipFile = new CompressFileGzip();
            gZipFile.gzipFile(filename, destinaton_zip_filepath);
    */
            
            //analyze the raw data file
            //analysis.readProbeFileOutputStats(file.getName());
            
            /*try
            {
                if(file.delete())
                {
                        System.out.println(file.getName() + " is deleted!");
                }else{
                        System.out.println("Delete operation is failed.");
                }

            }
            catch(Exception e)
            {
            	e.printStackTrace();
            }*/
        }
    }

    @Override
    public void close() {
        /*
         try {nz.close();} catch (Exception ignore){}
         */
        try {
            prbwp.close();
        } catch (Exception ignore) {
        }

    }
    
}
