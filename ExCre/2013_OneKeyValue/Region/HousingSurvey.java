/*
 #Full Name : Team4(Myat Oo, Zijian Zhang, Mohammad Islam
 #Course Number and Title: CSC643
 #Submission Date : April 12, 2017
 #Assignment Number: Assignment_3
 #Name:HousingSurvey.java
 */
package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class HousingSurvey {

       //**************************************************************************
   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
   
         //*** our variables are declared here
      private Text keyOfMap = new Text();
      private Text variables = new Text();
   
      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
      
         String stdString =value.toString().replaceAll("'","");
         String row[] = stdString.split(",");
         //String row[] = value.toString().split(",");
         
         //keys
         String region = row[3]; 
         String cityOrSub = row[2];
         String ownOrRent = row[25];
         
         //values
         double income = Double.parseDouble(row[21]);
         int ageOfHouseHold = Integer.parseInt(row[1]); 
         int numberOfPeople = Integer.parseInt(row[20]);
        
               
         keyOfMap.set(region);
         variables.set(Double.toString(income)+","+ Integer.toString(ageOfHouseHold)+","+Integer.toString(numberOfPeople)); 
         //variables.set(String.format("%1$.4f,%2$d",Double.toString(income),Integer.toString(ageOfHouseHold))); 
         
         output.collect(keyOfMap, variables);
      }
   }

      //**************************************************************************
   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
         Text value = new Text();
         
         double totalIncome = 0;
         int countIncome=0;
         double avgIncome =0;
         
         double totalAge =0;
         int countAge=0;
         double avgAge=0;
         
         double totalNum =0;
         int countNum=0;
         double avgNum=0;
                 
         while (values.hasNext()) {
            String tokens[] = values.next().toString().split(",");
            if(Double.parseDouble(tokens[0])!=-6){
               totalIncome += Double.parseDouble(tokens[0]);
               countIncome++;
            }     
            
            if(Double.parseDouble(tokens[0])!=-9){
            
               totalAge += Double.parseDouble(tokens[1]);
               countAge++;
            }
            
            if(Double.parseDouble(tokens[0])!=-6){
               totalNum += Double.parseDouble(tokens[2]);
               countNum++;
            }                
         }
         if(countIncome!=0){
            avgIncome=(double)totalIncome/countIncome;
         }
         
         if(countAge!=0){
            avgAge=(double)totalAge/countAge;
         }
         
         if(countNum!=0){
            avgNum=(double)totalNum/countNum;
         }
         value.set(String.format( "%.2f",avgIncome)+","+String.format( "%.2f", avgAge )+","+String.format( "%.2f", avgNum ));
         //value.set(Double.toString(avgIncome)+","+Double.toString(avgAge )+","+Double.toString(avgNum ));
         //value.set(String.format("%1$.4f,%2$.4f",avgIncome,avgAge));
         //output.collect(key, new Text(Integer.toString(tempMax)));
         output.collect(key,value);
      
      }
   }

      //**************************************************************************
   public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(HousingSurvey.class);
      conf.setJobName("HADS");
   
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);
   
      conf.setMapperClass(Map.class);
      conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);
   
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
   
      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
   
      JobClient.runJob(conf);
   }
}