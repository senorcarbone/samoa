package com.yahoo.labs.flink;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.IntOption;
import com.yahoo.labs.samoa.learners.InstanceContentEvent;
import com.yahoo.labs.samoa.moa.streams.InstanceStream;
import com.yahoo.labs.samoa.moa.streams.generators.RandomTreeGenerator;
import com.yahoo.labs.samoa.streams.PrequentialSourceProcessor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class TestSerialization {

	private static ClassOption streamTrainOption = new ClassOption("trainStream", 's', "Stream to learn from.", InstanceStream.class,
			RandomTreeGenerator.class.getName());
	private static IntOption instanceLimitOption = new IntOption("instanceLimit", 'i', "Maximum number of instances to test/train on  (-1 = no limit).", 1000000, -1,
			Integer.MAX_VALUE);
	private static IntOption sourceDelayOption = new IntOption("sourceDelay", 'w', "How many microseconds between injections of two instances.", 0, 0, Integer.MAX_VALUE);

	private static IntOption batchDelayOption = new IntOption("delayBatchSize", 'b', "The delay batch size: delay of x milliseconds after each batch ", 1, 1, Integer.MAX_VALUE);

	public static void main(String[] args) throws Exception {


		PrequentialSourceProcessor preqSource = new PrequentialSourceProcessor();
		preqSource.setStreamSource((InstanceStream) streamTrainOption.getValue());
		preqSource.setMaxNumInstances(instanceLimitOption.getValue());
		preqSource.setSourceDelay(sourceDelayOption.getValue());
		preqSource.setDelayBatchSize(batchDelayOption.getValue());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setDegreeOfParallelism(1);
		env.addSource(new MyInstanceGenerator(preqSource)).print();

		env.execute();

	}


	private static class MyInstanceGenerator implements org.apache.flink.streaming.api.function.source.SourceFunction<InstanceContentEvent> {

		private final PrequentialSourceProcessor proc;

		private MyInstanceGenerator(PrequentialSourceProcessor proc) {
			this.proc = proc;
		}

		@Override
		public void invoke(Collector<InstanceContentEvent> collector) throws Exception {
			proc.getDataset();
			while (this.proc.hasNext()) {
				collector.collect((InstanceContentEvent) proc.nextEvent());
			}
		}
	}


}


