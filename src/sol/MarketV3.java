package sol;

import java.util.ArrayList;

public class MarketV3 {
	public String marketName;
	
	public double avgCoverage;
	public double avgError;
	
	
	ArrayList<Double> coverages; //each value is the coverage for a epoch
	ArrayList<Double> errors; //each value is the error for each epoch
	
	/**
	 * variables for stats in an epoch
	 */
		public int noOfTMCsInOneEpoch; 
		public int noOfCoveredTMCsInOneEpoch;
		public double errorInOneEpoch;
	
}
