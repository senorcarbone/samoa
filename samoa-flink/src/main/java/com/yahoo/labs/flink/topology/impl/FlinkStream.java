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
import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.topology.AbstractStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;


/**
 * A stream for SAMOA based on Apache Flink's DataStream
 */
public class FlinkStream extends AbstractStream implements FlinkComponent {

	private static int outputCounter = 0;
	private FlinkComponent procItem;
	private DataStream dataStream;

	public FlinkStream(FlinkComponent sourcePi) {
		this.procItem = sourcePi;
		setStreamId(String.valueOf(outputCounter++));
		initialise();
	}

	@Override
	public void initialise() {

		if (procItem instanceof FlinkProcessingItem) {
			dataStream = ((FlinkProcessingItem) procItem).getOutStream().select(getStreamId()).map(new MapFunction<Utils.SamoaType, Utils.SamoaType>() {
				@Override
				public Utils.SamoaType map(Utils.SamoaType samoaType) throws Exception {
					return samoaType;
				}
			});
		} else
			dataStream = procItem.getOutStream();
	}

	@Override
	public boolean canBeInitialised() {
		return procItem.isInitialised();
	}

	@Override
	public boolean isInitialised() {
		return dataStream != null;
	}

	@Override
	public DataStream getOutStream() {
		return dataStream;
	}


	@Override
	public void put(ContentEvent event) {
		((FlinkProcessingItem) procItem).putToStream(event, this);
	}

}
