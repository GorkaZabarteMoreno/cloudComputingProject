/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main.java.es.upm.fi.cloud.YellowTaxiTrip;

import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;

import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;







public class CongestedArea{
	
	
	
	
	
	
	
	
	
    public static void main(String[] args){

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
        
		
		DataStream<String> s = env.readTextFile("/main/resources/taxis.csv");
        String outFilePath = "/YellowTaxiTrip/target";
        
        SingleOutputStreamOperator<Tuple19<String,String,String,Integer,Double,String,Boolean,String,String,String,Double,Double,Double,Double,Double,Double,Double,Double,Double>> s2 = 
        		s.flatMap(new FlatMapFunction<String,
        				Tuple19<String,String,String,Integer,Double,String,Boolean,String,String,String,Double,Double,Double,Double,Double,Double,Double,Double,Double>>() {
        	@Override
        	public void flatMap(String input, 
        			Collector<Tuple19<String,String,String,Integer,Double,String,Boolean,String,String,String,Double,Double,Double,Double,Double,Double,Double,Double,Double>> collector) 
        					throws Exception {
        		String[] taxi = input.split(",");
        		collector.collect(new Tuple19<String,String,String,Integer,Double,String,Boolean,String,String,String,Double,Double,Double,Double,Double,Double,Double,Double,Double>
        		(taxi[0],taxi[1],taxi[2],Integer.parseInt(taxi[3]),Double.parseDouble(taxi[4]),taxi[5],Boolean.parseBoolean(taxi[6]),taxi[7],taxi[8],taxi[9],Double.parseDouble(taxi[10]),
        		Double.parseDouble(taxi[11]),Double.parseDouble(taxi[12]),Double.parseDouble(taxi[13]),Double.parseDouble(taxi[14]),Double.parseDouble(taxi[15]),Double.parseDouble(taxi[16]),
        		Double.parseDouble(taxi[17]),Double.parseDouble(taxi[18])));
        	}
        });
        
        
        
        try{
        	env.execute();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}

