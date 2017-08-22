package net.nzcorp.hbase.tableevent_signaler;
/**
 * Copyright 2017 NovoZymes A/S
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import com.google.common.io.Files;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.qpid.server.Broker;
import org.apache.qpid.server.BrokerOptions;
import org.json.JSONObject;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.tap4j.ext.junit.runner.TapRunnerClass;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The tests in this test suite uses the junit unittest framework, but is really an integration test in disguise.
 * We start up a hbase minicluster (in {@link TableEventSignalerTest#setupHBase(Map)} and a minimal AMQP
 * implementation in {@link TableEventSignalerTest#brokerStarter}, both running in memory.
 */
@RunWith(TapRunnerClass.class)
public class TableEventSignalerTest {
    private static final String amq_port_number = "25672";
    private static final String amq_uri = String.format("amqp://guest:guest@0.0.0.0:%s", amq_port_number);
    private static final String amq_default_address = String.format("%s/%s", amq_uri, "default");
    private static final String primaryTableNameString = "genome";
    private static final String secondaryIdxTableNameString = "genome_index";
    private static final String downstreamTableNameString = "assembly";
    private static final TableName primaryTableName = TableName.valueOf(primaryTableNameString);
    private static final TableName secondaryIdxTableName = TableName.valueOf(secondaryIdxTableNameString);
    private static final TableName downstreamTableName = TableName.valueOf(downstreamTableNameString);
    private static Table primaryTable;
    private static Table secondaryIdxTable;
    private static Table downstreamTable;

    private static final SimpleCache cache = new SimpleCache();

    private static BrokerManager brokerStarter;
    private static HBaseTestingUtility testingUtility = new HBaseTestingUtility();

    /**
     * Disable all loggers except for the coprocessor class. If you would like to silence the log output alltogether,
     * remove the line with the coprocessor name in it
     */
    @BeforeClass
    public static void beforeClass() throws Exception {
        List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for (Logger logger : loggers) {
            logger.setLevel(Level.ERROR);
        }
        LogManager.getLogger(TableEventSignaler.class).setLevel(Level.TRACE);
        brokerStarter = new BrokerManager();
        brokerStarter.startBroker();
        Map<String, String> kvs = configureHBase("some_key");
        setupHBase(kvs);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        primaryTable = null;
        testingUtility.shutdownMiniCluster();
        testingUtility = null;
        brokerStarter.shutdown();
    }

    @After
    public void after() throws Exception {
        cache.clear();
    }

    /**
     * emulate the hbase-site.xml for setting up the coprocessor
     */
    private static Map<String, String> configureHBase(String filterQs) {
        Map<String, String> kvs = new HashMap<>();
        kvs.put("destination_table", primaryTableNameString);
        kvs.put("secondary_index_table", secondaryIdxTableNameString);
        kvs.put("source_column_family", "e");
        kvs.put("target_column_family", "eg");
        kvs.put("secondary_index_cf", "a");
        kvs.put("amq_address", amq_default_address);
        kvs.put("queue_name", primaryTableNameString);
        kvs.put("send_value", "true");
        kvs.put("use_ssl", "true");
	if( filterQs.length() > 0 ){
	    kvs.put("filter_qualifiers", filterQs);
        }
	return kvs;
    }

    /**
     * Set up the cluster, create tables + column families and install coprocessor
     *
     * @param kvs
     * @throws Exception
     */
    private static void setupHBase(Map<String, String> kvs) throws Exception {

        // Get random port number
        testingUtility.getConfiguration().setInt("hbase.regionserver.port", HBaseTestingUtility.randomFreePort());
        testingUtility.getConfiguration().setInt("hbase.master.info.port", HBaseTestingUtility.randomFreePort());
        testingUtility.getConfiguration().setInt("hbase.client.scanner.timeout.period", 1000);
        testingUtility.getConfiguration().setInt("hbase.client.retries.number", 2);
        testingUtility.getConfiguration().setInt("hbase.client.pause", 1);
        testingUtility.getConfiguration().setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);

        // Start up the testing cluster. No mocking around here!
        testingUtility.startMiniCluster();

        HBaseAdmin hbAdmin = testingUtility.getHBaseAdmin();

        HTableDescriptor secIdxTableDesc = new HTableDescriptor(secondaryIdxTableName);
        secIdxTableDesc.addFamily(new HColumnDescriptor("a"));
        hbAdmin.createTable(secIdxTableDesc);

        HTableDescriptor primaryTableDesc = new HTableDescriptor(primaryTableName);
        primaryTableDesc.addFamily(new HColumnDescriptor("e"));
        // Load the coprocessor on the primary table
        primaryTableDesc.addCoprocessor(TableEventSignaler.class.getName(), null, Coprocessor.PRIORITY_USER, kvs);
        hbAdmin.createTable(primaryTableDesc);

        HTableDescriptor downstreamTableDesc = new HTableDescriptor(downstreamTableName);
        downstreamTableDesc.addFamily(new HColumnDescriptor("eg".getBytes()));
        hbAdmin.createTable(downstreamTableDesc);

        secondaryIdxTable = testingUtility.getConnection().getTable(secondaryIdxTableName);
        primaryTable = testingUtility.getConnection().getTable(primaryTableName);
        downstreamTable = testingUtility.getConnection().getTable(downstreamTableName);
    }


    static class BrokerManager {
        private final Broker b = new Broker();
        private final String configFileName = "qpid-config.json";
        private final String passwordFileName = "passwd.properties";


        public void startBroker() throws Exception {
                final BrokerOptions brokerOptions = new BrokerOptions();
                brokerOptions.setConfigProperty("qpid.pass_file", getResourcePath(passwordFileName));
                brokerOptions.setConfigProperty("qpid.work_dir", Files.createTempDir().getAbsolutePath());
                brokerOptions.setConfigProperty("qpid.amqp_port", amq_port_number);
                brokerOptions.setConfigProperty("qpid.keystorepath", getResourcePath("clientkeystore"));
                brokerOptions.setInitialConfigurationLocation(getResourcePath(configFileName));

                b.startup(brokerOptions);
        }

        public void shutdown(){
            b.shutdown();
        }

        private String getResourcePath(String filename) {
            ClassLoader classLoader = getClass().getClassLoader();
            File file = new File(classLoader.getResource(filename).getFile());
            return file.getAbsolutePath();
        }
    }

    public static class CacheEntry {
        private String text;
        private int sequenceNr;

        public CacheEntry(String text, int sequenceNr) {
            this.text = text;
            this.sequenceNr = sequenceNr;
        }

        public String getText() {
            return text;
        }
    }

    public static class SimpleCache {

        private List<CacheEntry> cache = new ArrayList<>();// CopyOnWriteArrayList<>();
        private AtomicInteger counter = new AtomicInteger(0);

        public void update(String text) {
            cache.add(new CacheEntry(text, counter.getAndIncrement()));
        }

        public List<CacheEntry> getContent() {
            return Collections.unmodifiableList(cache);
        }

        public void clear(){
            cache.clear();
        }
    }

    public class Receiver {

        public Receiver() {}
        public void receive() throws Exception {
            ConnectionFactory factory = createConnectionFactory();

            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(primaryTableNameString, true, false, false, null);
            channel.basicConsume(primaryTableNameString, true, newConsumer(channel));
        }

        protected ConnectionFactory createConnectionFactory() throws Exception {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(amq_uri);
            factory.useSslProtocol();
            return factory;
        }

        private DefaultConsumer newConsumer(Channel channel) {
            return new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) throws IOException {
                    cache.update(new String(body));
                }
            };
        }
    }

    @Test
    public void prePutHappyCase() throws Exception {
        System.out.println("prePutHappyCase");
        //simulate population of secondary index as a result of the above
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);

        // Add a column to the primary table, which should trigger a data ripple to the downstream table
        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn("e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);

        Receiver rec = new Receiver();
        rec.receive();

        int counter = 0;
        while(true) {
            Thread.sleep(500);
            List<CacheEntry> content = cache.getContent();
            if(content.size() == 0) {
                counter++;
                if(counter >10){
                    Assert.fail("Timeout while waiting for message broker");
                    break;
                }
                continue;
            }

            Assert.assertEquals(1, content.size());
            JSONObject jsonObject = new JSONObject(content.get(0).getText());
            String rowkey = (String) jsonObject.get("row_key");
            Assert.assertEquals("EFG1", rowkey);
            break;
        }
    }

    @Test
    public void verifyValueNotSentByDefault() throws Exception {
        System.out.println("verifyValueNotSentByDefault");
        //simulate population of secondary index as a result of the above
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);

        // Add a column to the primary table, which should trigger a data ripple to the downstream table
        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn("e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);

        new Receiver().receive();
        int counter = 0;
        while(true) {
            Thread.sleep(500);
            List<CacheEntry> content = cache.getContent();
            if(content.size() == 0) {
                counter++;
                if(counter >10){
                    Assert.fail("Timeout while waiting for message broker");
                    break;
                }
                continue;
            }

            Assert.assertEquals(1, content.size());
            JSONObject jsonObject = new JSONObject(content.get(0).getText());
            String rowkey = (String) jsonObject.get("row_key");
            Assert.assertEquals("EFG1", rowkey);
            break;
        }
    }

    @Test
    public void filterOnQualifiers() throws Exception {
        System.out.println("filterOnQualifiers");
        //simulate population of secondary index as a result of the above
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);

        // Add a column to the primary table, which should trigger a message in the queue
        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn("e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);

        // Add a column to the primary table which should _not_ trigger a message
        Put anotherPut = new Put("EFG1".getBytes());
        anotherPut.addColumn("e".getBytes(), "foo_bar".getBytes(), "baz".getBytes());
        primaryTable.put(anotherPut);

        // Verify that the queue only contains a single message
        new Receiver().receive();
        int counter = 0;
        while(true) {
            Thread.sleep(500);
            List<CacheEntry> content = cache.getContent();
            if(content.size() == 0) {
                counter++;
                if(counter >10){
                    Assert.fail("Timeout while waiting for message broker");
                    break;
                }
                continue;
            }

            Assert.assertEquals(1, content.size());
            JSONObject jsonObject = new JSONObject(content.get(0).getText());
            String rowkey = (String) jsonObject.get("row_key");
            Assert.assertEquals("EFG1", rowkey);
            break;
        }
    }

    @Test
    public void onlyPutWhenInQualifierFilter() throws Exception {
        System.out.println("onlyPutWhenInQualifierFilter");
        //simulate population of secondary index as a result of the above
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);

        // Add a column to the primary table, which should trigger a data ripple to the downstream table
        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn("e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        tablePut.addColumn("e".getBytes(), "other_key".getBytes(), "some_value".getBytes());
        tablePut.addColumn("e".getBytes(), "third_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);

        new Receiver().receive();
        int counter = 0;
        while(true) {
            Thread.sleep(500);
            List<CacheEntry> content = cache.getContent();
            if(content.size() == 0) {
                counter++;
                if(counter >10){
                    Assert.fail("Timeout while waiting for message broker");
                    break;
                }
                continue;
            }

            Assert.assertEquals(1, content.size());
            JSONObject jsonObject = new JSONObject(content.get(0).getText());
            String rowkey = (String) jsonObject.get("row_key");
            Assert.assertEquals("EFG1", rowkey);
            break;
        }
    }


    @Test
    public void preDeleteHappyCase() throws Exception {
        System.out.println("preDeleteHappyCase");
        //simulate population of secondary index for a put on the downstreamTable
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);

        //simulate a data rippling performed by the data_rippler service consuming off of the rabbitmq queue
        Put tablePut = new Put("EFB1".getBytes());
        tablePut.addColumn("eg".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        downstreamTable.put(tablePut);

        // finished with the setup, we now issue a delete which should be caught by the rabbitmq
        Delete d = new Delete("EFG1".getBytes());
        primaryTable.delete(d);


        new Receiver().receive();
        int counter = 0;
        while(true) {
            Thread.sleep(500);
            List<CacheEntry> content = cache.getContent();
            if(content.size() == 0) {
                counter++;
                if(counter >10){
                    Assert.fail("Timeout while waiting for message broker");
                    break;
                }
                continue;
            }

            Assert.assertEquals(1, content.size());
            JSONObject jsonObject = new JSONObject(content.get(0).getText());
            String rowkey = (String) jsonObject.get("row_key");
            Assert.assertEquals("EFG1", rowkey);
            break;
        }
    }

    @Test
    public void discernNewPutFromUpdate() throws Exception {
        System.out.println("discernNewPutFromUpdate");
        //simulate population of secondary index as a result of the above
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);

        /*
            The first put should be registered as a "put" action, while the second should be registered as an "update"
            action, thereby signalling different action to be taken by the consumers
         */

        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn("e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);

        tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn("e".getBytes(), "some_other_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);
        new Receiver().receive();
        int counter = 0;
        while(true) {
            Thread.sleep(500);
            List<CacheEntry> content = cache.getContent();
            if(content.size() == 0) {
                counter++;
                if(counter >10){
                    Assert.fail("Timeout while waiting for message broker");
                    break;
                }
                continue;
            }

            Assert.assertEquals(1, content.size());
            JSONObject jsonObject = new JSONObject(content.get(0).getText());
            String rowkey = (String) jsonObject.get("row_key");
            Assert.assertEquals("EFG1", rowkey);
            break;
        }
    }
}
