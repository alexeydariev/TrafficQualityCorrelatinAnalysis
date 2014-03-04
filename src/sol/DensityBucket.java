package sol;

import java.util.ArrayList;

public class DensityBucket {
  double lowerBound;
  double upperBound;
	
  double avgProbeCnt;
  double avgGroundTruthSpeed;
  double avgProbeCntPerMile;
  double avgError;
  double avgQualityScore;
  double pairCnt;
  double cntOfPairFallOutBand;
  ArrayList<EpochTMC> pairs;
  
  public DensityBucket(){
	  pairs=new ArrayList<EpochTMC>();
  }
  
  public double[] getPairGroundTruthSpeedArray(){
	  double[] ret=new double[pairs.size()];
	  for(int i=0;i<pairs.size();i++){
		  ret[i]=pairs.get(i).groundTruthSpeed;
	  }
	  return ret;
  }
  
  public double[] getPairErrorArray(){
	  double[] ret=new double[pairs.size()];
	  for(int i=0;i<pairs.size();i++){
		  ret[i]=pairs.get(i).error;
	  }
	  return ret;
  }
}
