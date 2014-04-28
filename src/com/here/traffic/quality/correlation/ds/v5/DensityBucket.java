package com.here.traffic.quality.correlation.ds.v5;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.here.traffic.quality.correlation.ds.v4.EpochTMC;

public class DensityBucket {
  public double lowerBound;
  public double upperBound;
	
  public double avgProbeCnt;
  public double avgProbSpdStd;
  public double avgProbeSpdMean;
  
  public double avgGroundTruthSpeed;
  public double avgProbeCntPerMile;
  public double avgError;
  public double stdError;
  
  public double avgQualityScore;
  public int pairCnt;
  public double cntOfPairFallOutBand;
  public ArrayList<EpochTMC> pairs;
  
  public double[] groundTruthSpeed;
  
  
  
  public HashSet<String> providerSet;
  public int providerSetSize;
  
  public DensityBucket(){
	  pairs=new ArrayList<EpochTMC>();
	  providerSet=new HashSet<String>();
  }
 
  
}
