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

package Y;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;


/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet <Tuple8<String, String, String, String, String, String, String, Integer>> corridas = env.readTextFile("/tmp/cab-flink.txt").map(
				new MapFunction<String, Tuple8<String, String, String, String, String, String, String, String>>() {
					@Override
					public Tuple8<String, String, String, String, String, String, String, String> map(String s) throws Exception {
						String[] line = s.split(",");
						return new Tuple8<String, String, String, String, String, String, String, String>(line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7]);
					}
				}).filter(
				new FilterFunction<Tuple8<String, String, String, String, String, String, String, String>>() {
					@Override
					public boolean filter(Tuple8<String, String, String, String, String, String, String, String> d) throws Exception {
						return d.f4.equals("yes");
				}}
		).map(
				new MapFunction<Tuple8<String, String, String, String, String, String, String, String>, Tuple8<String, String, String, String, String, String, String, Integer>>() {
					@Override
					public Tuple8<String, String, String, String, String, String, String, Integer> map(Tuple8<String, String, String, String, String, String, String, String> d) throws Exception {
						return new Tuple8<String, String, String, String, String, String, String, Integer>(d.f0, d.f1, d.f2, d.f3, d.f4, d.f5, d.f6, Integer.parseInt(d.f7));
					}
				}
		);





		DataSet <Tuple2<String, Integer>> destino = corridas.map(new MapFunction<Tuple8<String, String, String, String, String, String, String, Integer>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Tuple8<String, String, String, String, String, String, String, Integer> input) throws Exception {
				return new Tuple2<String, Integer>(input.f6, input.f7);
			}
		});

		DataSet <Tuple3<String, Integer, Integer>> origem = corridas.map(new MapFunction<Tuple8<String, String, String, String, String, String, String, Integer>, Tuple3<String, Integer, Integer>>() {
			@Override
			public Tuple3<String, Integer, Integer> map(Tuple8<String, String, String, String, String, String, String, Integer> d) throws Exception {
				return new Tuple3<String, Integer, Integer>(d.f5, d.f7, 1);
			}
		});

		DataSet<Tuple3<String, Integer, Integer>> number_passagers = origem.groupBy(0).sum(1).andSum(2);

		DataSet<Tuple2<String, Float>> avg_trip = number_passagers.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Float>>() {
			@Override
			public Tuple2<String, Float> map(Tuple3<String, Integer, Integer> d) throws Exception {
				Float result = (float) d.f1/d.f2;
				return new Tuple2<String, Float>(d.f0, result);
			}
		});


		DataSet <Tuple3<String, Integer, Integer>> trip_motorista = corridas.map(new MapFunction<Tuple8<String, String, String, String, String, String, String, Integer>, Tuple3<String, Integer, Integer>>() {
			@Override
			public Tuple3<String, Integer, Integer> map(Tuple8<String, String, String, String, String, String, String, Integer> d) throws Exception {
				return new Tuple3<String, Integer, Integer>(d.f0, d.f7, 1);
			}
		}).groupBy(0).sum(1).andSum(2);

		DataSet <Tuple2<String, Float>> viagens_group = trip_motorista.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Float>>() {
			@Override
			public Tuple2<String, Float> map(Tuple3<String, Integer, Integer> d) throws Exception {
				Float result = (float) d.f1 / d.f2;
				return new Tuple2<String, Float>(d.f0, result);
			}
		});




		DataSet <Tuple2<String, Integer>> count_corridas = destino.groupBy(0).sum(1);

		count_corridas.writeAsCsv("/tmp/popular_trip.csv", "\n", ";");

		avg_trip.writeAsCsv("/tmp/passager_trip.csv","\n", ";");

		viagens_group.writeAsCsv("/tmp/viagem_motorista.csv", "\n", ";");

		env.execute("Flink Batch Java API Skeleton");
	}
}
