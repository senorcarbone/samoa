package com.yahoo.labs.flink.topology.impl;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 - 2015 Yahoo! Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import com.yahoo.labs.flink.Utils;
import com.yahoo.labs.samoa.core.EntranceProcessor;
import com.yahoo.labs.samoa.topology.AbstractEntranceProcessingItem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class FlinkEntranceProcessingItem extends AbstractEntranceProcessingItem
		implements FlinkComponent {

	private DataStream outStream;

	public FlinkEntranceProcessingItem(EntranceProcessor proc) {
		super(proc);
	}

	@Override
	public void initialise() {
		outStream = StreamExecutionEnvironment.getExecutionEnvironment().addSource(new SourceFunction() {
			@Override
			public void invoke(Collector collector) throws Exception {
				EntranceProcessor proc = getProcessor();
				while (proc.hasNext()) {
					collector.collect(new Utils.SamoaType(proc.nextEvent(), getName()));
				}
			}
		});
	}

	@Override
	public boolean canBeInitialised() {
		return true;
	}

	@Override
	public boolean isInitialised() {
		return outStream != null;
	}

	@Override
	public DataStream getOutStream() {
		return outStream;
	}
}
