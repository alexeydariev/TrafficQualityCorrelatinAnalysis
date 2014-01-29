package sol;

public class EpochTMC {
	public static int EPOCH_DURATION=2; //in minutes
	
	public int epochIdx; //index of epoch of a day, e.g. 1 means the 1st epoch of the day
	public String tmc; //tmc code
	
	public boolean covered; //true if have real-time data
	public int noOfProbes;
	
	public double error;//diff between the GT and the predicated speed
	
	
}
