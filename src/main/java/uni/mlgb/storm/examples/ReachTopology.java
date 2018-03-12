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

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.util.*;

/**
 * This is a good example of doing complex Distributed RPC on top of Storm. This program creates a topology that can
 * compute the reach for any URL on Twitter in realtime by parallelizing the whole computation.
 * <p/>
 * Reach is the number of unique people exposed to a URL on Twitter. To compute reach, you have to get all the people
 * who tweeted the URL, get all the followers of all those people, unique that set of followers, and then count the
 * unique set. It's an intense computation that can involve thousands of database calls and tens of millions of follower
 * records.
 * <p/>
 * This Storm topology does every piece of that computation in parallel, turning what would be a computation that takes
 * minutes on a single machine into one that takes just a couple seconds.
 * <p/>
 * For the purposes of demonstration, this topology replaces the use of actual DBs with in-memory hashmaps.
 *
 * @see <a href="http://storm.apache.org/documentation/Distributed-RPC.html">Distributed RPC</a>
 */
public class ReachTopology {
    public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {{
        put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
        put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
        put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
    }};

    public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
        put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
        put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
        put("tim", Arrays.asList("alex"));
        put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
        put("adam", Arrays.asList("david", "carissa"));
        put("mike", Arrays.asList("john", "bob"));
        put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
    }};

    public static class GetTweeters extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            // ID for each function call, e.g., request ID.
            Object id = tuple.getValue(0);
            String url = tuple.getString(1);
            List<String> tweeters = TWEETERS_DB.get(url);
            if (tweeters != null) {
                for (String tweeter : tweeters) {
                    collector.emit(new Values(id, tweeter));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "tweeter"));
        }
    }

    public static class GetFollowers extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            String tweeter = tuple.getString(1);
            List<String> followers = FOLLOWERS_DB.get(tweeter);
            if (followers != null) {
                for (String follower : followers) {
                    collector.emit(new Values(id, follower));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "follower"));
        }
    }

    public static class PartialUniquer extends BaseBatchBolt<Object> {
        BatchOutputCollector _collector;
        // ID for each batch operation. Here it is same as the ID for each function call.
        Object _id;
        Set<String> _followers = new HashSet<String>();

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            _followers.add(tuple.getString(1));
        }

        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _followers.size()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "partial-count"));
        }
    }

    public static class CountAggregator extends BaseBatchBolt<Object> {
        BatchOutputCollector _collector;
        Object _id;
        int _count = 0;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            _count += tuple.getInteger(1);
        }

        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "reach"));
        }
    }

    public static LinearDRPCTopologyBuilder construct() {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");
        builder.addBolt(new GetTweeters(), 4);
        builder.addBolt(new GetFollowers(), 12).shuffleGrouping();
        // It is parallel for each ID.
        builder.addBolt(new PartialUniquer(), 6).fieldsGrouping(new Fields("id", "follower"));
        builder.addBolt(new CountAggregator(), 3).fieldsGrouping(new Fields("id"));
        return builder;
    }

    public static void main(String[] args) {
        LinearDRPCTopologyBuilder builder = construct();

        Config conf = new Config();
        conf.setNumWorkers(1);
        String topoName = "reach-drpc";
        if (args.length > 0) {
            topoName = args[0];
        }
        try {
            StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createRemoteTopology());
            requestDrpc(conf);
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
            requestDrpc(conf);
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
    }

    // FIXME docker iamge storm 1.2.1 does not include other dependencies such as Thrift.
    // You should also start DRPC server before submit this drpc topology.
    public static void requestDrpc(Config conf) {
        try (DRPCClient drpc = DRPCClient.getConfiguredClient(conf)) {
            String[] urlsToTry = new String[]{ "foo.com/blog/1", "engineering.twitter.com/blog/5", "notaurl.com" };
            for (String url : urlsToTry) {
                System.out.println("Reach of " + url + ": " + drpc.execute("reach", url));
            }
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (DRPCExecutionException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
