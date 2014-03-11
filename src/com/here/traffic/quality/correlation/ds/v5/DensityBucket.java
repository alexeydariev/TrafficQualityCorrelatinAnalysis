package com.here.traffic.quality.correlation.ds.v5;

import java.util.ArrayList;

import com.here.traffic.quality.correlation.ds.v4.EpochTMC;

public class DensityBucket {
  public double lowerBound;
  public double upperBound;
	
  public double avgProbeCnt;
  public double avgGroundTruthSpeed;
  public double avgProbeCntPerMile;
  public double avgError;
  public double stdError;
  
  public double avgQualityScore;
  public double pairCnt;
  public double cntOfPairFallOutBand;
  public ArrayList<EpochTMC> pairs;
  
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
