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


import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction.Context;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;














public class CongestedArea{
	
	private static class CongestionAggregate
	implements AggregateFunction<Tuple4<LocalDateTime,LocalDateTime,Double,Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>> {
	@Override
	public Tuple2<Integer, Double> createAccumulator() {
		return new Tuple2<>(0, 0.0);
		}

	@Override
	public Tuple2<Integer, Double> add(Tuple4<LocalDateTime,LocalDateTime,Double,Double> value, Tuple2<Integer, Double> accumulator) {
		return new Tuple2<>(accumulator.f0 + 1, accumulator.f1+value.f2);
		}

	@Override
	public Tuple2<Integer, Double> getResult(Tuple2<Integer, Double> accumulator) {
		return new Tuple2<>(accumulator.f0, accumulator.f1/accumulator.f0);
		}

	@Override
	public Tuple2<Integer, Double> merge(Tuple2< Integer, Double> a, Tuple2<Integer, Double> b) {
		return new Tuple2<>(a.f0+b.f0, a.f1+b.f1);
		}
	}
	
	
	private static class CongestionProcessing extends ProcessAllWindowFunction<Tuple2<Integer, Double>, Tuple3<String, Integer, Double>, TimeWindow>{
		
		public void process(Context context,
							Iterable<Tuple2<Integer,Double>> accum_out,
							Collector<Tuple3<String,Integer,Double>> out) {
			
			Tuple2<Integer,Double> avg = accum_out.iterator().next();
			Long start_millis = context.window().getStart();
			LocalDateTime start_date = LocalDateTime.ofInstant(Instant.ofEpochMilli(start_millis), ZoneId.of("Europe/Madrid"));
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
			String start_date_text = start_date.format(formatter);
			out.collect(new Tuple3<String,Integer,Double>(start_date_text,avg.f0,avg.f1));
		}
		

	}
	
	
    public static void main(String[] args){

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		final ParameterTool params = ParameterTool.fromArgs(args);
		
        
        DataStream<String> s = env.readTextFile(params.get("input"));
        String outFilePath = params.get("output");
		        
        SingleOutputStreamOperator<Tuple4<LocalDateTime,LocalDateTime,Double,Double>> s_parsed = 
        		s.flatMap(new FlatMapFunction<String,
        				Tuple4<LocalDateTime,LocalDateTime,Double,Double>>() {
        	@Override
        	public void flatMap(String input, Collector<Tuple4<LocalDateTime,LocalDateTime,Double,Double>> collector) throws Exception {
        		String[] taxi = input.split(",",-1);
        		for (int i = 0;i<taxi.length;i++) {
	        		if (taxi[i].isEmpty()) {
	        			taxi[i] = "0.0";
	        		}
        		}
        		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        		//OPTION 1: Amount charged computed by me
        		/*
        		collector.collect(new Tuple4<LocalDateTime,LocalDateTime,Double,Double>
        		(LocalDateTime.parse(taxi[1],formatter),LocalDateTime.parse(taxi[2],formatter),
        				Double.parseDouble(taxi[10])+Double.parseDouble(taxi[11])+Double.parseDouble(taxi[12])+Double.parseDouble(taxi[14])+Double.parseDouble(taxi[15])+Double.parseDouble(taxi[17])+Double.parseDouble(taxi[18]),
        				Double.parseDouble(taxi[17])));*/
        		//OPTION 2: Amount charged using total_amount
        		collector.collect(new Tuple4<LocalDateTime,LocalDateTime,Double,Double>
        		(LocalDateTime.parse(taxi[1],formatter),LocalDateTime.parse(taxi[2],formatter),
        				Double.parseDouble(taxi[16]),Double.parseDouble(taxi[17])));
        	}
        });
        
        /*SingleOutputStreamOperator<Tuple4<LocalDateTime,LocalDateTime,Double,Double>> s_timestamped =
        		s_parsed.assignTimestampsAndWatermarks(
        			new AscendingTimestampExtractor<Tuple4<LocalDateTime,LocalDateTime,Double,Double>>() {
                    public long extractAscendingTimestamp(Tuple4<LocalDateTime,LocalDateTime,Double,Double> element){
                        return element.f0.atZone(ZoneId.of("Europe/Madrid")).toInstant().toEpochMilli(); //*1000 to set seconds. Flink takes ms as default so if we have 30s we need 30000ms
                    }});*/
        
        SingleOutputStreamOperator<Tuple4<LocalDateTime,LocalDateTime,Double,Double>> s_timestamped =
        		s_parsed.assignTimestampsAndWatermarks(
        			WatermarkStrategy.<Tuple4<LocalDateTime,LocalDateTime,Double,Double>>forMonotonousTimestamps().withTimestampAssigner((event,timestamp)->event.f0.atZone(ZoneId.of("America/New_York")).toInstant().toEpochMilli())
        				);
        
        
        
        //Filter tuples for surcharge > 0. When surcharge is = 0, that trip has not underwent a congested area and therefore
        //lacks of interest.
        SingleOutputStreamOperator<Tuple4<LocalDateTime,LocalDateTime,Double,Double>> s_congested = 
        		s_timestamped.filter(new FilterFunction<Tuple4<LocalDateTime,LocalDateTime,Double,Double>>() {
        	@Override
        	public boolean filter(Tuple4<LocalDateTime,LocalDateTime,Double,Double> s_aux) throws Exception {
        			return s_aux.f3>0.0;
        	}
        });
        

        
        
        SingleOutputStreamOperator<Tuple3<String,Integer,Double>> s_out = s_congested.windowAll(TumblingEventTimeWindows.of(Time.days(1))) // 4 or 5?
        																				.aggregate(new CongestionAggregate(), new CongestionProcessing());				
        		//.aggregate(new CongestionAggregate(), new CongestionProcessing());
        
        
        
        s_out.writeAsCsv(outFilePath+"/output/congested.csv",FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        
        
        
        
        
        try{
        	env.execute();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}

