/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uni.mlgb.storm.examples;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 * FIXME Storm docker 1.2.1 bug: cannot load class which extends ConfigurableTopology.
 */
public class ExclamationTopology extends ConfigurableTopology {

  public static class ExclamationBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

  }

  public static void main(String[] args) throws Exception {
    System.out.println(ExclamationTopology.class.getName() + " loaded.");
    Thread.sleep(1000*3);
    ConfigurableTopology.start(new ExclamationTopology(), args);
  }

  protected int run(String[] args) {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(), 10);
    builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
    builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");

    conf.setDebug(true);

    String topologyName = "test";

    conf.setNumWorkers(3);

    if (args != null && args.length > 0) {
      topologyName = args[0];
    }

    return submit(topologyName, conf, builder);
  }
}
