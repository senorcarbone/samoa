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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.yahoo.labs.samoa.topology.AbstractTopology;
import com.yahoo.labs.samoa.topology.EntranceProcessingItem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A SAMOA topology on Apache Flink
 */
public class FlinkTopology extends AbstractTopology {

	private final StreamExecutionEnvironment env;

	public FlinkTopology(String name, StreamExecutionEnvironment env) {
		super(name);
		this.env = env;
	}

	public StreamExecutionEnvironment getEnvironment() {
		return env;
	}

	public void build() {

		for (EntranceProcessingItem src : getEntranceProcessingItems()) {
			((FlinkEntranceProcessingItem) src).initialise();
		}

		initPIs(ImmutableList.copyOf((Iterable<? extends FlinkComponent>) getProcessingItems()));

	}

	private static void initPIs(ImmutableList<FlinkComponent> flinkComponents) {
		if (flinkComponents.isEmpty()) return;

		for (FlinkComponent comp : flinkComponents) {
			if (comp.canBeInitialised()) comp.initialise();
		}

		initPIs((ImmutableList<FlinkComponent>) Iterables.filter(flinkComponents, new com.google.common.base.Predicate<FlinkComponent>() {
			@Override
			public boolean apply(FlinkComponent flinkComponent) {
				return !flinkComponent.isInitialised();
			}
		}));
	}


}
