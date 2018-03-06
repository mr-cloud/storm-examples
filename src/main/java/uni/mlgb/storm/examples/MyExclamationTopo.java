package uni.mlgb.storm.examples;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * @author leo
 */
public class MyExclamationTopo {

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

    public static void main (String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        System.out.println(MyExclamationTopo.class.getName() + " is Running.");
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word", new TestWordSpout(), 10);
        builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
        builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");

        // create the default config object
        Config conf = new Config();

        conf.setDebug(true);

        if (args != null && args.length > 0) {

            // run it in a live cluster

            // set the number of workers for running all spout and bolt tasks
            conf.setNumWorkers(3);

            // create the topology and submit with config
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

        } else {
            // FIXME Storm docker 1.2.1 bug: cannot support local cluster.
            String topologyName = "test";

            // run it in a simulated local cluster

            // set the number of threads to run - similar to setting number of workers in live cluster
            conf.setMaxTaskParallelism(3);

            // create the local cluster instance
            LocalCluster cluster = null;
            try {
                cluster = new LocalCluster();
                // submit the topology to the local cluster
                cluster.submitTopology(topologyName, conf, builder.createTopology());
                System.out.println("Submit with local mode.");
                Thread.sleep(1000*3);
                // let the topology run for 10 seconds. note topologies never terminate!
                Utils.sleep(10000);

                // now kill the topology
                cluster.killTopology(topologyName);

                // we are done, so shutdown the local cluster
                System.out.println("Ready to shutdown.");
                Thread.sleep(1000*3);
                cluster.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
