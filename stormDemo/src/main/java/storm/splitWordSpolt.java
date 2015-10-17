/**
 * 
 */
package storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/** 
 * @ClassName: splitWordSpolt 
 * @Description: Strom 原生测试
 * @date 2015-4-30 
 */
public class splitWordSpolt extends BaseRichSpout{


    public static Logger LOG = LoggerFactory.getLogger(TestWordSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;

    public splitWordSpolt() {
        this(true);
    }

    public splitWordSpolt(boolean isDistributed) {
        _isDistributed = isDistributed;
    }
        
    /**
     * {"storm.id" "test-1-1431052819", "dev.zookeeper.path" "/tmp/dev-storm-zookeeper", 
     * "topology.tick.tuple.freq.secs" 30, "topology.builtin.metrics.bucket.size.secs" 60,
     *  "topology.fall.back.on.java.serialization" true, "topology.max.error.report.per.interval" 5, 
     *  "zmq.linger.millis" 0, "topology.skip.missing.kryo.registrations" true, "storm.messaging.netty.client_worker_threads" 1, 
     *  "ui.childopts" "-Xmx768m", "storm.zookeeper.session.timeout" 20000, "nimbus.reassign" true, 
     *  "topology.trident.batch.emit.interval.millis" 50, "storm.messaging.netty.flush.check.interval.ms" 10, "nimbus.monitor.freq.secs" 10, "logviewer.childopts" "-Xmx128m", "java.library.path" "/usr/local/lib:/opt/local/lib:/usr/lib", "topology.executor.send.buffer.size" 1024, "storm.local.dir" "C:\\Users\\ADMINI~1\\AppData\\Local\\Temp\\1e547438-fb30-4ce9-afdc-e65ccaed65b2", "storm.messaging.netty.buffer_size" 5242880, "supervisor.worker.start.timeout.secs" 120, "topology.enable.message.timeouts" true, "nimbus.cleanup.inbox.freq.secs" 600, "nimbus.inbox.jar.expiration.secs" 3600, "drpc.worker.threads" 64, "storm.meta.serialization.delegate" "backtype.storm.serialization.DefaultSerializationDelegate", "topology.worker.shared.thread.pool.size" 4, "nimbus.host" "localhost", "storm.messaging.netty.min_wait_ms" 100, "storm.zookeeper.port" 2000, "transactional.zookeeper.port" nil, "topology.executor.receive.buffer.size" 1024, "transactional.zookeeper.servers" nil, "storm.zookeeper.root" "/storm", "storm.zookeeper.retry.intervalceiling.millis" 30000, "supervisor.enable" true, "storm.messaging.netty.server_worker_threads" 1, "storm.zookeeper.servers" ["localhost"], "transactional.zookeeper.root" "/transactional", "topology.acker.executors" nil, "topology.kryo.decorators" (), "topology.name" "test", "topology.transfer.buffer.size" 1024, "topology.worker.childopts" nil, "drpc.queue.size" 128, "worker.childopts" "-Xmx768m", "supervisor.heartbeat.frequency.secs" 5, "topology.error.throttle.interval.secs" 10, "zmq.hwm" 0, "drpc.port" 3772, "supervisor.monitor.frequency.secs" 3, "drpc.childopts" "-Xmx768m", "topology.receiver.buffer.size" 8, "task.heartbeat.frequency.secs" 3, "topology.tasks" nil, "storm.messaging.netty.max_retries" 300, "topology.spout.wait.strategy" "backtype.storm.spout.SleepSpoutWaitStrategy", "nimbus.thrift.max_buffer_size" 1048576, "topology.max.spout.pending" nil, "storm.zookeeper.retry.interval" 1000, "topology.sleep.spout.wait.strategy.time.ms" 1, "nimbus.topology.validator" "backtype.storm.nimbus.DefaultTopologyValidator", "supervisor.slots.ports" (1027 1028 1029), "topology.environment" nil, "topology.debug" true, "nimbus.task.launch.secs" 120, "nimbus.supervisor.timeout.secs" 60, "topology.kryo.register" nil, "topology.message.timeout.secs" 30, "task.refresh.poll.secs" 10, "topology.workers" 2, "supervisor.childopts" "-Xmx256m", "nimbus.thrift.port" 6627, "topology.stats.sample.rate" 0.05, "worker.heartbeat.frequency.secs" 1, "topology.tuple.serializer" "backtype.storm.serialization.types.ListDelegateSerializer", "topology.disruptor.wait.strategy" "com.lmax.disruptor.BlockingWaitStrategy", "topology.multilang.serializer" "backtype.storm.multilang.JsonSerializer", "nimbus.task.timeout.secs" 30, "storm.zookeeper.connection.timeout" 15000, "topology.kryo.factory" "backtype.storm.serialization.DefaultKryoFactory", "drpc.invocations.port" 3773, "logviewer.port" 8000, "zmq.threads" 1, "storm.zookeeper.retry.times" 5, "topology.worker.receiver.thread.count" 1, "storm.thrift.transport" "backtype.storm.security.auth.SimpleTransportPlugin", "topology.state.synchronization.timeout.secs" 60, "supervisor.worker.timeout.secs" 30, "nimbus.file.copy.expiration.secs" 600, "storm.messaging.transport" "backtype.storm.messaging.netty.Context", "logviewer.appender.name" "A1", "storm.messaging.netty.max_wait_ms" 1000, "drpc.request.timeout.secs" 600, "storm.local.mode.zmq" false, "ui.port" 8080, "nimbus.childopts" "-Xmx1024m", "storm.cluster.mode" "local", 
     * "topology.max.task.parallelism" nil, "storm.messaging.netty.transfer.batch.size" 262144, "topology.classpath" nil}
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        System.out.println("splitWordSpoltConf=========="+conf);
    }
    
    public void close() {
        
    }
    
    int i = 1;
    public void nextTuple() {
        Utils.sleep(100);
//	    if(i <2){
	    	
//        String sb = "hello wold hello china ";
	        String sb = "1 2 3 4 5 ";
	//        sb.concat("the cow jumped over the moon");
	//        sb.concat("the man went to the store and bought some candy");
	//       tuple.add("how many apples can you eat");
	    /*    final String[] words = new String[] {"nathan", "mike", "jackson", "golda", "bertels"};
	        final Random rand = new Random();
	        final String word = words[rand.nextInt(words.length)];*/
	        System.out.print("Hello bingbing=======================================================");
	        _collector.emit(new Values(sb),"123456789");
	        System.out.println(this);
//	    }
//	    i++;
    }
    
    public void ack(Object msgId) {
    	System.out.println("OK:"+msgId);
    }

    public void fail(Object msgId) {
    	  System.out.println("FAIL:"+msgId);
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    /**
     * builder.setSpout("1_spout",spout);时候执行
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }    

}
