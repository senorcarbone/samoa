package com.yahoo.labs.flink;

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


import com.yahoo.labs.samoa.core.ContentEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

public class Utils {

	public static final String LOCAL_MODE = "local";
	public static final String REMOTE_MODE = "remote";

	// FLAGS
	public static final String MODE_FLAG = "--mode";
	public static final String DEFAULT_PARALLELISM = "--parallelism";

	//config values
	public static boolean isLocal = true;
	public static String flinkMaster;
	public static int flinkPort;
	public static String[] dependecyJars;
	public static int parallelism = 1;

	public enum Partitioning {SHUFFLE, ALL, GROUP}

	public static class SamoaType extends Tuple3<String, ContentEvent, String> {
		public SamoaType(ContentEvent event, String streamId) {
			super(event.getKey(), event, streamId);
		}
	}

	public static DataStream subscribe(DataStream<SamoaType> stream, Partitioning partitioning) {
		switch (partitioning) {
			case ALL:
				return stream.broadcast();
			case GROUP:
				return stream.groupBy(0);
			case SHUFFLE:
			default:
				return stream.shuffle();
		}
	}

	public static void extractFlinkArguments(List<String> tmpargs) {
		for (int i = tmpargs.size() - 1; i >= 0; i--) {
			String arg = tmpargs.get(i).trim();
			String[] splitted = arg.split("=", 2);

			if (splitted.length >= 2) {
				if (MODE_FLAG.equals(splitted[0])) {
					isLocal = LOCAL_MODE.equals(splitted[1]);
					tmpargs.remove(i);
				}
			}
		}
	}

}
