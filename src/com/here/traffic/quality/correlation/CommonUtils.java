package com.here.traffic.quality.correlation;


import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.Variance;


public class CommonUtils {
	public static void main(String[] args){
		
		System.out.println(calculatePDFOfNormalDistribution(0.06, 0.08, -0.001));
		
		System.out.println(new NormalDistribution(0.06, 0.08).density(-0.001));
		//System.out.println(stringTimeToSeconds("18:06:34"));
		//System.out.println(secondsToStringTime(65181));
		//System.out.println(getDirectory("E:/workspace/android/ActivityRecognition/logs/weka/input/features2013_08_295.arff"));
		//checkAccelerometerTrainingFile();
		
		//System.out.println(String.format("%-6.3f%-6.3f", 3.31, -1.12));
		
		//System.out.println(new Date(12363992261L));
		
		/*String groundTruth="10:35:20~10:36:47 10:38:00~10:39:35 10:41:08~10:42:35 10:44:22~10:45:50 10:47:35~10:48:46";
		for(String duration: groundTruth.split(" ")){
			for(String timestamp: duration.split("~")){
				System.out.println(HMSToSeconds(timestamp));
				
			}
		}
		
		groundTruth="41893 49475 49476 49568 49653 49761";
		for(String timestamp: groundTruth.split(" ")){
			System.out.println(secondsToHMS(Integer.parseInt((timestamp)) ));			
		}
		System.out.println(secondsToHMS(67442));*/
		
		//System.out.println(calPDFOfNormalDistribution(0.1, 0.4, 0.7));
		
		//Double[][]lists=new Double[][]{{-3.162, -0.049, 0.120 }, { -1.387,  0.327,  4.264}};
		
		//System.out.println(calculatePearsonCorrelation(Arrays.asList(lists[0]), Arrays.asList(lists[1])));
		//System.out.println(calculateCosineSimilarity(Arrays.asList(lists[0]), Arrays.asList(lists[1])));
	}
	
	public static double[] maxAndMin(double[] values){
		if(values.length==0) return null; 
		double max=values[0],min=values[0];
		for(int i=1;i<values.length;i++){
			max=Math.max(max, values[i]);
			min=Math.min(min, values[i]);
		}
		return new double[]{min, max};
	}

	
	public static double max(double[] values){
		if(values.length==0) return Integer.MIN_VALUE; 
		double max=values[0];
		for(int i=1;i<values.length;i++){
			max=Math.max(max, values[i]);
		}
		return max;
	}
	
	public static double min(double[] values){
		if(values.length==0) return Integer.MIN_VALUE; 
		double min=values[0];
		for(int i=1;i<values.length;i++){
			min=Math.min(min, values[i]);
		}
		return min;
	}
	
	public static double[] calculateHistogram(List<Double> values, double[] normalizedInterval, int noOfBins){		
		double[] doubles=new double[values.size()];
		for(int i=0;i<values.size();i++) doubles[i]=values.get(i);
		return calculateHistogram(doubles, normalizedInterval, noOfBins);
	}
	
	public static double[] calculateHistogram(double[] values, double[] normalizedInterval, int noOfBins){		
		if(values.length<1) return null;
		if(noOfBins<2) return new double[]{1};
		
		double min=normalizedInterval[0], max=normalizedInterval[1];
		double[] probs=new double[noOfBins];
		for(int i=0;i<values.length;i++){
			probs[(int)((values[i]-min)*noOfBins/(max-min))]+=1;
		}
		for(int i=0;i<probs.length;i++){
			probs[i]/=values.length;
		}
		return probs;
	}
	
	public static double calculatePearsonCorrelation(List<Double> list1, List<Double> list2){
		if(list1.size()!=list2.size()){
			System.err.println("Two lists must have the same dimensionality.");
			return 0;
		}
		double mean1=calculateMean(list1);
		double mean2=calculateMean(list2);
		
		double std1=Math.sqrt(calculateVariance(list1, mean1));
		double std2=Math.sqrt(calculateVariance(list2, mean2));
		
		double dividend=0;
		for(int i=0;i<list1.size();i++){
			dividend+=(list1.get(i)-mean1)*(list2.get(i)-mean2);
		}
		dividend/=list1.size()-1;
			
		//System.out.println(mean1+" "+std1+" "+mean2+" "+std2+" "+dividend);
		return dividend/(std1*std2);
	}
	
	public static double calculateCosineSimilarity(List<Double> list1, List<Double> list2){
		if(list1.size()!=list2.size()){
			System.err.println("Two lists must have the same dimensionality.");
			return 0;
		}
		double dividend=0, divisor1=0, divisor2=0;
	
		for(int i=0;i<list1.size();i++){
			dividend+=list1.get(i)*list2.get(i);
			divisor1+=Math.pow(list1.get(i),2);
			divisor2+=Math.pow(list2.get(i),2);
		}
		//System.out.println(dividend+" "+divisor1+" "+divisor2);
		return dividend/(Math.sqrt(divisor1)*Math.sqrt(divisor2));		
	}
	
	public static double calculatePDFOfNormalDistribution(double mean, double std, double value){
		double prob=Math.pow(Math.E, -Math.pow(value-mean, 2)/2/Math.pow(std, 2))/(Math.sqrt(Math.PI*2)*std);
		if(prob>1) System.out.println(mean+" "+std+" "+value);
		return prob;
	}
	
	public static double calculateMean(List<Double> list){
		if(list==null||list.size()==0) return Double.MAX_VALUE;
		double sum=0;		
		for(double num: list) sum+=num;
		return sum/list.size();		
	}
	
	public static double calculateVariance(List<Double> list, double mean){
		if(mean==Double.MAX_VALUE) return mean;
		if(list.size()==1) return 0;
		double sum=0;
		for(double num: list){
			sum+=Math.pow(num-mean, 2);
		}
		return sum/(list.size()-1);
	}
	
	public static double[] doubleListToDoubleArray(List<Double> values){
		if(values==null) return null;
		double[] ret=new double[values.size()];
		for(int i=0; i<ret.length;i++) ret[i]=values.get(i);
		return ret;
	}
	
	public static String getFileName(String path){
		int idx = path.lastIndexOf("/");
		return idx >= 0 ? path.substring(idx + 1) : path;
	}
	
	public static String getDirectory(String path){
		int idx = path.lastIndexOf("/");
		return idx >= 0 ? path.substring(0,idx + 1) : path;
	}
	
	public static int HMSToSeconds(String time){
		int secs=-1;
		try{
			String[] fields=time.split(":");
			secs=Integer.parseInt(fields[0])*3600+Integer.parseInt(fields[1])*60;
			if (fields.length>2) secs+=Integer.parseInt(fields[2]);
		}catch(Exception ex){
			System.out.println("time="+time);
			ex.printStackTrace();
			System.exit(-1);
		}
		return secs;
	}
	
	public static long HMSSToMillseconds(String time){
		String[] fields=time.split(":");
		long millisecs=(Integer.parseInt(fields[0])*3600+Integer.parseInt(fields[1])*60)*1000;
		if (fields.length>2) millisecs+=Integer.parseInt(fields[2])*1000;	
		if (fields.length>3) millisecs+=Integer.parseInt(fields[3]);
		return millisecs;
	}
	
	public static String secondsToHMS(int secs){
		StringBuilder  sb=new StringBuilder();
		int[] hourMinSec=new int[3];
		hourMinSec[0]=secs/3600;
		hourMinSec[1]=(secs-hourMinSec[0]*3600)/60;
		hourMinSec[2]=secs-hourMinSec[0]*3600-hourMinSec[1]*60;
		for(int i=0;i<hourMinSec.length;i++){			
			if(i==hourMinSec.length-1&&hourMinSec[i]==0) continue;
			if(i>0) sb.append(":");
			sb.append(hourMinSec[i]>=10?hourMinSec[i]:("0"+hourMinSec[i]));

		}
		return sb.toString();
	}
	
	
	/*
	 * @start: index of the starting field (inclusive)
	 * @end: index of the ending field (exclusive)
	 */
	public static String join(String[] fields, int start, int end, String connectingDelimeter){
		try{
			StringBuilder sb=new StringBuilder();
			for(int i=start;i<end;i++){
				if(i!=start) sb.append(connectingDelimeter);
				sb.append(fields[i]);
			}
			return sb.toString();
		}catch(Exception ex){
			ex.printStackTrace();
			return null;
		}
	}
	
	
	
	/*
	 * return a substring consisting of fields of the given string at the given
	 * indices  
	 * @start: index of the starting field (inclusive)
	 * @end: index of the ending field (exclusive)
	 */
	public static String cutField(String s, String delimeter, int start, int end, String connectingDelimeter){
		String[] fields=s.split(delimeter);
		return join(fields, start, end, connectingDelimeter);
	}
	
	public static String cutField(String s, String delimeter, int start){
		return cutField(s, delimeter, start, s.split(delimeter).length, " ");
	}
	
	
	
	
	public static double[] intToDoubleArray(int[] arr){
		if (arr == null) return null;
		int n = arr.length;
		double[] ret = new double[n];
		for (int i = 0; i < n; i++) {
			ret[i] = (double)arr[i];
		}
		return ret;
	}
	
	public static float[] doubleToFloatArray(double[] arr) {
		if (arr == null) return null;
		int n = arr.length;
		float[] ret = new float[n];
		for (int i = 0; i < n; i++) {
			ret[i] = (float)arr[i];
		}
		return ret;
	}

	public static double[] floatToDoubleArray(float[] arr) {
		if (arr == null) return null;
		int n = arr.length;
		double[] ret = new double[n];
		for (int i = 0; i < n; i++) {
			ret[i] = (double)arr[i];
		}
		return ret;
	}
	
		
	/**
	 * 
	 * @param fields
	 * @param vars
	 * @param avgs
	 * @param j
	 * @return the first field that out of the 3\delta range
	 */
	private static int  isAbnormal(String [] fields, ArrayList<ArrayList<Double>> vars, ArrayList<ArrayList<Double>> avgs, int j){
		int[] indices={2,3, 5, 6, 7 ,8};
		for(int i=0;i<indices.length;i++){
			if(Math.abs(Double.parseDouble(fields[indices[i]])-avgs.get(j).get(indices[i]-1))>=3*Math.sqrt(vars.get(j).get(indices[i]-1)) ){
				return i;
			}
		}
		//System.out.println(isAbnormal);
		return -1;
	}
	
}

