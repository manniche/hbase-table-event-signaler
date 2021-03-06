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
import com.rabbitmq.client.GetResponse;
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
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The tests in this test suite uses the junit unittest framework, but is really an integration test in disguise.
 * We start up a hbase minicluster (in {@link TableEventSignalerTest#setupHBase(Map)} and a minimal AMQP
 * implementation in {@link TableEventSignalerTest#embeddedAMQPBroker}, both running in memory and torn down
 * after each test
 *
 * @throws Exception
 */
@RunWith(MockitoJUnitRunner.class)
public class TableEventSignalerTest {
    private Broker broker;
    private static final String amq_default_address = "amqp://guest:guest@0.0.0.0:25672/hbase_events";
    private static final String primaryTableNameString = "genome";
    private static final String secondaryIdxTableNameString = "genome_index";
    private static final String downstreamTableNameString = "assembly";
    private static final TableName primaryTableName = TableName.valueOf(primaryTableNameString);
    private static final TableName secondaryIdxTableName = TableName.valueOf(secondaryIdxTableNameString);
    private static final TableName downstreamTableName = TableName.valueOf(downstreamTableNameString);
    private Table primaryTable;
    private Table secondaryIdxTable;
    private Table downstreamTable;

    private HBaseTestingUtility testingUtility = new HBaseTestingUtility();

    /**
     * Disable all loggers except for the coprocessor class. If you would like to silence the log output alltogether,
     * remove the line with the coprocessor name in it
     */
    @BeforeClass
    public static void beforeClass() {
        List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for (Logger logger : loggers) {
            logger.setLevel(Level.ERROR);
        }
        LogManager.getLogger(TableEventSignaler.class).setLevel(Level.TRACE);
    }

    /**
     * emulate the hbase-site.xml, while allowing for the individual test to start up a cluster with a special
     * configuration for the coprocessor
     *
     * @param destination_table
     * @param sec_idx_table
     * @param src_cf
     * @param trgt_cf
     * @param sec_idx_cf
     * @param amq_addr
     * @param send_value
     * @return
     */
    private Map<String, String> configureHBase(String destination_table,
                                               String sec_idx_table,
                                               String src_cf,
                                               String trgt_cf,
                                               String sec_idx_cf,
                                               String amq_addr,
                                               String queueName,
                                               String send_value,
					       String filterQs) {
        Map<String, String> kvs = new HashMap<>();
        kvs.put("destination_table", destination_table);
        kvs.put("source_column_family", src_cf);
        kvs.put("secondary_index_table", sec_idx_table);
        kvs.put("target_column_family", trgt_cf);
        kvs.put("secondary_index_cf", sec_idx_cf);
        kvs.put("amq_address", amq_addr);
        kvs.put("queue_name", queueName);
        kvs.put("send_value", send_value);
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
    private void setupHBase(Map<String, String> kvs) throws Exception {

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

    @After
    public void after() throws Exception {
        primaryTable = null;
        testingUtility.shutdownMiniCluster();
        testingUtility = null;
    }

    @Rule
    public final ExternalResource embeddedAMQPBroker = new ExternalResource() {

        @Override
        protected void before() throws Throwable {
            final String configFileName = "qpid-config.json";
            final String passwordFileName = "passwd.properties";

            getResourcePath(configFileName);
            getResourcePath(passwordFileName);
            BrokerOptions brokerOptions = new BrokerOptions();
            brokerOptions.setConfigProperty("qpid.amqp_port", "25672");
            brokerOptions.setConfigProperty("qpid.pass_file", getResourcePath(passwordFileName));
            brokerOptions.setConfigProperty("qpid.work_dir", Files.createTempDir().getAbsolutePath());
            brokerOptions.setInitialConfigurationLocation(getResourcePath(configFileName));
            brokerOptions.setStartupLoggedToSystemOut(true);

            // start broker
            broker = new Broker();
            broker.startup(brokerOptions);
        }

        @Override
        protected void after() {
            broker.shutdown();
        }

        private String getResourcePath(String filename) {
            ClassLoader classLoader = getClass().getClassLoader();
            File file = new File(classLoader.getResource(filename).getFile());
            return file.getAbsolutePath();
        }
    };


    @Test
    public void prePutHappyCase() throws Exception {

        Map<String, String> kvs = configureHBase(primaryTableNameString, secondaryIdxTableNameString, "e", "eg", "a", amq_default_address, primaryTableNameString, "true", "");
        setupHBase(kvs);

        //simulate population of secondary index as a result of the above
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);

        // Add a column to the primary table, which should trigger a data ripple to the downstream table
        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn("e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);

        //check that values made it to the queue
        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
        factory.setUri(amq_default_address);
        com.rabbitmq.client.Connection conn = factory.newConnection();
        com.rabbitmq.client.Channel channel = conn.createChannel();
        System.out.println(String.format("Test: connecting to %s", primaryTableNameString));

        while (true) {
            GetResponse response = channel.basicGet(primaryTableNameString, false);
            if (response == null)//busy-wait until the message has made it through the MQ
            {
                continue;
            }
            String routingKey = response.getEnvelope().getRoutingKey();
            Assert.assertEquals("Routing key should be rowkey", "genome", routingKey);

            String contentType = response.getProps().getContentType();
            Assert.assertEquals("Content type should be preserved", "application/json", contentType);

            Map<String, Object> headers = response.getProps().getHeaders();
            Assert.assertEquals("An action should be set on the message", "put", headers.get("action").toString());

            byte[] body = response.getBody();

            JSONObject jo = new JSONObject(new String(body));
            String column_family = (String) jo.get("column_family");
            Assert.assertEquals("Column family should be preserved in the message body", "eg", column_family);

            String column_value = (String) jo.get("column_value");
            Assert.assertEquals("Column value should be preserved in the message body", "some_value", column_value);

            long deliveryTag = response.getEnvelope().getDeliveryTag();
            channel.basicAck(deliveryTag, false);
            break;
        }
    }

    @Test
    public void verifyValueNotSentByDefault() throws Exception {

        Map<String, String> kvs = configureHBase(primaryTableNameString, secondaryIdxTableNameString, "e", "eg", "a", amq_default_address, primaryTableNameString, "false", "");
        setupHBase(kvs);

        //simulate population of secondary index as a result of the above
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);

        // Add a column to the primary table, which should trigger a data ripple to the downstream table
        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn("e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);

        //check that values made it to the queue
        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
        factory.setUri(amq_default_address);
        com.rabbitmq.client.Connection conn = factory.newConnection();
        com.rabbitmq.client.Channel channel = conn.createChannel();
        System.out.println(String.format("Test: connecting to %s", primaryTableNameString));

        while (true) {
            GetResponse response = channel.basicGet(primaryTableNameString, false);
            if (response == null)//busy-wait until the message has made it through the MQ
            {
                continue;
            }
            String routingKey = response.getEnvelope().getRoutingKey();
            Assert.assertEquals("Routing key should be rowkey", "genome", routingKey);

            String contentType = response.getProps().getContentType();
            Assert.assertEquals("Content type should be preserved", "application/json", contentType);

            Map<String, Object> headers = response.getProps().getHeaders();
            Assert.assertEquals("An action should be set on the message", "put", headers.get("action").toString());

            byte[] body = response.getBody();

            JSONObject jo = new JSONObject(new String(body));
            String column_family = (String) jo.get("column_family");
            Assert.assertEquals("Column family should be preserved in the message body", "eg", column_family);

            String column_value = (String) jo.get("column_value");
            Assert.assertEquals("Column value is not sent by default", "", column_value);

            long deliveryTag = response.getEnvelope().getDeliveryTag();
            channel.basicAck(deliveryTag, false);
            break;
        }
    }

    @Test
    public void filterOnQualifiers() throws Exception {

        Map<String, String> kvs = configureHBase(primaryTableNameString, secondaryIdxTableNameString, "e", "eg", "a", amq_default_address, primaryTableNameString, "false", "one_key|some_key");
        setupHBase(kvs);

        //simulate population of secondary index as a result of the above
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);

        // Add a column to the primary table, which should trigger a data ripple to the downstream table
        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn("e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);

        //check that values made it to the queue
        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
        factory.setUri(amq_default_address);
        com.rabbitmq.client.Connection conn = factory.newConnection();
        com.rabbitmq.client.Channel channel = conn.createChannel();
        System.out.println(String.format("Test: connecting to %s", primaryTableNameString));

        while (true) {
            GetResponse response = channel.basicGet(primaryTableNameString, false);
            if (response == null)//busy-wait until the message has made it through the MQ
            {
                continue;
            }
            String routingKey = response.getEnvelope().getRoutingKey();
            Assert.assertEquals("Routing key should be rowkey", "genome", routingKey);

            String contentType = response.getProps().getContentType();
            Assert.assertEquals("Content type should be preserved", "application/json", contentType);

            Map<String, Object> headers = response.getProps().getHeaders();
            Assert.assertEquals("An action should be set on the message", "put", headers.get("action").toString());

            byte[] body = response.getBody();

            JSONObject jo = new JSONObject(new String(body));
            String column_family = (String) jo.get("column_family");
            Assert.assertEquals("Column family should be preserved in the message body", "eg", column_family);

            String column_value = (String) jo.get("column_value");
            Assert.assertEquals("Column value is not sent by default", "", column_value);

            long deliveryTag = response.getEnvelope().getDeliveryTag();
            channel.basicAck(deliveryTag, false);
            break;
        }
    }

    @Test
    public void onlyPutWhenInQualifierFilter() throws Exception {

        Map<String, String> kvs = configureHBase(primaryTableNameString, secondaryIdxTableNameString, "e", "eg", "a", amq_default_address, primaryTableNameString, "false", "some_key");
        setupHBase(kvs);

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

        //check that values made it to the queue
        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
        factory.setUri(amq_default_address);
        com.rabbitmq.client.Connection conn = factory.newConnection();
        com.rabbitmq.client.Channel channel = conn.createChannel();
        System.out.println(String.format("Test: connecting to %s", primaryTableNameString));

        while (true) {
	    int numMessages = channel.queueDeclarePassive(primaryTableNameString).getMessageCount();
            GetResponse response = channel.basicGet(primaryTableNameString, false);
            if (response == null || numMessages == 0)//busy-wait until the message has made it through the MQ
            {
                continue;
            }
	    Assert.assertEquals("There should be only a single key in the queue", 1, numMessages);
            String routingKey = response.getEnvelope().getRoutingKey();
            Assert.assertEquals("Routing key should be rowkey", "genome", routingKey);

            String contentType = response.getProps().getContentType();
            Assert.assertEquals("Content type should be preserved", "application/json", contentType);

            Map<String, Object> headers = response.getProps().getHeaders();
            Assert.assertEquals("An action should be set on the message", "put", headers.get("action").toString());

            byte[] body = response.getBody();

            JSONObject jo = new JSONObject(new String(body));
            String column_family = (String) jo.get("column_family");
            Assert.assertEquals("Column family should be preserved in the message body", "eg", column_family);

            String column_value = (String) jo.get("column_value");
            Assert.assertEquals("Column value is not sent by default", "", column_value);

            long deliveryTag = response.getEnvelope().getDeliveryTag();
            channel.basicAck(deliveryTag, false);
            break;
        }
    }


    
    @Test(expected = IOException.class)
    public void filterOnQualifiersThatDoesNotExistSilencesEvents() throws Exception {

        Map<String, String> kvs = configureHBase(primaryTableNameString, secondaryIdxTableNameString, "e", "eg", "a", amq_default_address, primaryTableNameString, "false", "one|some");
        setupHBase(kvs);

        //simulate population of secondary index as a result of the above
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);

        // Add a column to the primary table, which should trigger a data ripple to the downstream table
        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn("e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);

        //check that values made it to the queue
        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
        factory.setUri(amq_default_address);
        com.rabbitmq.client.Connection conn = factory.newConnection();
        com.rabbitmq.client.Channel channel = conn.createChannel();

	
	System.out.println(String.format("is ok? %s", channel.queueDeclarePassive(primaryTableNameString)));
    }

    
    @Test
    public void prePostErrorOnUnavailableAMQP() throws Exception {
        Map<String, String> kvs = configureHBase(primaryTableNameString, secondaryIdxTableNameString, "e", "eg", "a", amq_default_address, primaryTableNameString, "true", "");
        setupHBase(kvs);
        broker.shutdown(13); //unlucky broker

        //simulate population of secondary index as a result of the above
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);

        // Add a column to the primary table, which should trigger a data ripple to the downstream table
        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn("e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        try {
            primaryTable.put(tablePut);
        } catch (RetriesExhaustedWithDetailsException e) {
            Assert.assertEquals("Exception type should be java.net.ConnectException", java.net.ConnectException.class, e.getCause(0).getClass());
            Assert.assertEquals("Exception text should be present", "Failed 1 action: ConnectException: 1 time, ", e.getMessage());
        }
    }

    @Test
    public void preDeleteHappyCase() throws Exception {

        Map<String, String> kvs = configureHBase(primaryTableNameString, secondaryIdxTableNameString, "e", "eg", "a", amq_default_address, primaryTableNameString, "true", "");
        setupHBase(kvs);
        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
        factory.setUri(amq_default_address);
        com.rabbitmq.client.Connection conn = factory.newConnection();
        com.rabbitmq.client.Channel channel = conn.createChannel();


        //simulate population of secondary index for a put on the downstreamTable
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);

        //simulate a data rippling performed by the data_rippler service consuming off of the rabbitmq queue
        Put tablePut = new Put("EFB1".getBytes());
        tablePut.addColumn("eg".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        downstreamTable.put(tablePut);

        // since we made no active put to the queue from the prePut, we need to declare it explicitly here
        channel.queueDeclare(primaryTableNameString, true, false, false, null);

        // finished with the setup, we now issue a delete which should be caught by the rabbitmq
        Delete d = new Delete("EFG1".getBytes());
        primaryTable.delete(d);

        //check that values made it to the queue
        while (true) {
            GetResponse response = channel.basicGet(primaryTableNameString, false);
            if (response == null)//busy-wait until the message has made it through the MQ
            {
                continue;
            }
            String routingKey = response.getEnvelope().getRoutingKey();
            Assert.assertEquals("Routing key should be rowkey", "genome", routingKey);

            String contentType = response.getProps().getContentType();
            Assert.assertEquals("Content type should be preserved", "application/json", contentType);

            Map<String, Object> headers = response.getProps().getHeaders();
            Assert.assertEquals("An action should be set on the message", "delete", headers.get("action").toString());

            byte[] body = response.getBody();
            JSONObject jo = new JSONObject(new String(body));

            String column_qualifier = (String) jo.get("column_qualifier");
            Assert.assertEquals("Column qualifier should be empty, signalling a row delete", "", column_qualifier);

            long deliveryTag = response.getEnvelope().getDeliveryTag();
            channel.basicAck(deliveryTag, false);
            break;
        }
    }

    @Test
    public void discernNewPutFromUpdate() throws Exception {
        Map<String, String> kvs = configureHBase(primaryTableNameString, secondaryIdxTableNameString, "e", "eg", "a", amq_default_address, primaryTableNameString, "true", "");
        setupHBase(kvs);

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

        //check that values made it to the queue
        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
        factory.setUri(amq_default_address);
        com.rabbitmq.client.Connection conn = factory.newConnection();
        com.rabbitmq.client.Channel channel = conn.createChannel();
        int msgs_to_consume = 2;
        while (msgs_to_consume > 0) {
            System.out.println(String.format("Messages to get: %s", msgs_to_consume));
            GetResponse response = channel.basicGet(primaryTableNameString, false);
            if (response == null)//busy-wait until the message has made it through the MQ
            {
                continue;
            }
            String routingKey = response.getEnvelope().getRoutingKey();
            Assert.assertEquals("Routing key should be rowkey", "genome", routingKey);

            String contentType = response.getProps().getContentType();
            Assert.assertEquals("Content type should be preserved", "application/json", contentType);

            Map<String, Object> headers = response.getProps().getHeaders();
            byte[] body = response.getBody();

            JSONObject jo = new JSONObject(new String(body));
            String column_family = (String) jo.get("column_family");
            Assert.assertEquals("Column family should be preserved in the message body", "eg", column_family);

            String column_value = (String) jo.get("column_qualifier");

            if(headers.get("action").toString().equals("update")){
                Assert.assertEquals("Column value should be preserved in the message body", "some_other_key", column_value);
            }
            else {
                Assert.assertEquals("Column value should be preserved in the message body", "some_key", column_value);
            }

            long deliveryTag = response.getEnvelope().getDeliveryTag();
            channel.basicAck(deliveryTag, false);
            msgs_to_consume--;
        }
    }


    @Test
    public void preDeleteErrorOnUnavailableAMQP() throws Exception {
        Map<String, String> kvs = configureHBase(primaryTableNameString, secondaryIdxTableNameString, "e", "eg", "a", amq_default_address, primaryTableNameString, "true", "");
        setupHBase(kvs);

        //simulate population of secondary index as a result of the above
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);

        // Add a column to the primary table, which should trigger a data ripple to the downstream table
        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn("e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);

        Delete d = new Delete("EFG1".getBytes());
        // finished with the setup, we now issue a delete which should be caught by the rabbitmq broker. Unfortunately,
        // it dies:
        broker.shutdown(13); //unlucky broker

        try {
            primaryTable.delete(d);
        } catch (RetriesExhaustedWithDetailsException e) {
            Assert.assertEquals("Exception type should be java.net.ConnectException", java.net.ConnectException.class, e.getCause(0).getClass());
            Assert.assertEquals("Exception text should be present", "Failed 1 action: ConnectException: 1 time, ", e.getMessage());
        } catch (RetriesExhaustedException e) {
            Assert.assertEquals("Exception type is? ", RetriesExhaustedException.class, e.getClass());
            Assert.assertEquals("Exception text should be present", "java.net.ConnectException", e.getCause().getMessage().substring(0, 25));
        }
    }
}
