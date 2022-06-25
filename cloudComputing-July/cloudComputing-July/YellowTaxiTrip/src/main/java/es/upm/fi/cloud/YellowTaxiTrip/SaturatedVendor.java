package main.java.es.upm.fi.cloud.YellowTaxiTrip;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction.Context;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class SaturatedVendor {
	
	private class saturatedAggregate implements AggregateFunction<Tuple3<String,LocalDateTime,LocalDateTime>,Tuple2<LocalDateTime,Integer>,Tuple2<LocalDateTime,Integer>>{
		@Override
		public Tuple2<LocalDateTime,Integer> createAccumulator() {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			LocalDateTime last = LocalDateTime.parse("1970-01-01 00:00:00",formatter);
			return new Tuple2<>(last, 0);
			}

		@Override
		public Tuple2<LocalDateTime, Integer> add(Tuple3<String,LocalDateTime,LocalDateTime> value, Tuple2<LocalDateTime,Integer> accumulator) {
			return new Tuple2<>(value.f2,accumulator.f1+1);
			}

		@Override
		public Tuple2<LocalDateTime, Integer> getResult(Tuple2<LocalDateTime, Integer> accumulator) {
			return new Tuple2<>(accumulator.f0, accumulator.f1);
			}

		@Override
		public Tuple2<LocalDateTime, Integer> merge(Tuple2< LocalDateTime, Integer> a, Tuple2<LocalDateTime, Integer> b) {
			return new Tuple2<>(a.f0, a.f1+b.f1);
			}
		}
	
	private static class saturatedProcessing extends ProcessWindowFunction<Tuple2<LocalDateTime, Integer>, Tuple4<String, String, String, Integer>, String, GlobalWindow>{
		
		public void process(Context context,
							String key,
							Iterable<Tuple2<LocalDateTime, Integer>> accum_out,
							Collector<Tuple4<String,LocalDateTime,LocalDateTime,Integer>> out) {
			
			Tuple2<LocalDateTime,Integer> avg = accum_out.iterator().next();
			Long start_millis = context.window().;
			LocalDateTime start_date = LocalDateTime.ofInstant(Instant.ofEpochMilli(start_millis), ZoneId.of("Europe/Madrid"));
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
			String start_date_text = start_date.format(formatter);
			String end_date_text = avg.f0.format(formatter);
			out.collect(new Tuple4<String, LocalDateTime, LocalDateTime, Integer>(key,start_date_text,end_date_text,avg.f1));
		}

		}
		

	}
	
	private class saturatedTrigger extends Trigger<Tuple3<String,LocalDateTime,LocalDateTime>,GlobalWindow>{
		
		private final ValueStateDescriptor<Long> stateDesc = new ValueStateDescriptor<>("previousTimer",LongSerializer.INSTANCE);

		@Override
		public void clear(GlobalWindow arg0, TriggerContext arg1) throws Exception {
			// TODO Auto-generated method stub	
			ValueState<Long> previousTimer = arg1.getPartitionedState(stateDesc);
			
			if (previousTimer.value() != null) {
				arg1.deleteEventTimeTimer(previousTimer.value());
				previousTimer.clear();
			}
		}

		@Override
		public TriggerResult onElement(Tuple3<String, LocalDateTime, LocalDateTime> arg0, long arg1, GlobalWindow arg2,
				TriggerContext arg3) throws Exception {
			//Delete previous timer if exists
			ValueState<Long> previousTimer = arg3.getPartitionedState(stateDesc);
			
			if (previousTimer.value() != null) {
				arg3.deleteEventTimeTimer(previousTimer.value());
				previousTimer.clear();
			}
			
			//Set new timer
			long timer = 10*60*1000;
			long targetTime = arg0.f2.atZone(ZoneId.of("Europe/Madrid")).toInstant().toEpochMilli()+timer;
			arg3.registerEventTimeTimer(targetTime);
			
			previousTimer.update(targetTime);
			
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onEventTime(long arg0, GlobalWindow arg1, TriggerContext arg2) throws Exception {
			// TODO Auto-generated method stub
			return TriggerResult.FIRE_AND_PURGE;
		}

		@Override
		public TriggerResult onProcessingTime(long arg0, GlobalWindow arg1, TriggerContext arg2) throws Exception {
			// TODO Auto-generated method stub
			return TriggerResult.CONTINUE;
		}

		
	}

	
	public static void main(String[] args) {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
        
        DataStream<String> s = env.readTextFile(args[0]);
        String outFilePath = args[1];
		        
        SingleOutputStreamOperator<Tuple3<String,LocalDateTime,LocalDateTime>> s_parsed = 
        		s.flatMap(new FlatMapFunction<String,
        				Tuple3<String,LocalDateTime,LocalDateTime>>() {
        	@Override
        	public void flatMap(String input, Collector<Tuple3<String,LocalDateTime,LocalDateTime>> collector) throws Exception {
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
        		collector.collect(new Tuple3<String,LocalDateTime,LocalDateTime>
        		(taxi[0],LocalDateTime.parse(taxi[1],formatter),LocalDateTime.parse(taxi[2],formatter)));
        	}
        });
        		
       
	    SingleOutputStreamOperator<Tuple4<LocalDateTime,LocalDateTime,Double,Double>> s_timestamped =
	    		s_parsed.assignTimestampsAndWatermarks(
	    			WatermarkStrategy.<Tuple3<String,LocalDateTime,LocalDateTime>>forMonotonousTimestamps().withTimestampAssigner((event,timestamp)->event.f1.atZone(ZoneId.of("Europe/Madrid")).toInstant().toEpochMilli())
	    				);
        		
        KeyedStream s_keyed = s_timestamped.keyBy(0).window(GlobalWindows.create())
        									   .trigger(new saturatedTrigger())
        									   .aggregate(new saturatedAggregate());
		

	}

}
