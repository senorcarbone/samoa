package com.yahoo.labs.flink.topology.impl;

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.topology.AbstractStream;
import com.yahoo.labs.samoa.topology.IProcessingItem;


/**
 * A stream for SAMOA based on Apache Flink's DataStream
 */
public class FlinkStream extends AbstractStream {

	public FlinkStream(IProcessingItem sourcePi) {
	}

	@Override
	public void put(ContentEvent event) {

	}
}
