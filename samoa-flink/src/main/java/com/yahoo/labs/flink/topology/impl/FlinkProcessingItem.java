package com.yahoo.labs.flink.topology.impl;

import com.yahoo.labs.samoa.topology.AbstractProcessingItem;
import com.yahoo.labs.samoa.topology.ProcessingItem;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.utils.PartitioningScheme;

import java.io.Serializable;


public class FlinkProcessingItem extends AbstractProcessingItem implements FlinkProcessingNode, Serializable{
	@Override
	protected ProcessingItem addInputStream(Stream inputStream, PartitioningScheme scheme) {
		return null;
	}
}
