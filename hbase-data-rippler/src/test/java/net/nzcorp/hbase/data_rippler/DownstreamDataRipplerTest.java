package net.nzcorp.hbase.data_rippler;

import com.google.common.io.Files;
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

@RunWith(MockitoJUnitRunner.class)
public class DownstreamDataRipplerTest {
    private Broker broker;
    private static final String amq_default_address = "::1";
    private static final String secondaryIdxTableNameString = "genome_index";
    private static final TableName primaryTableName = TableName.valueOf("genome");
    private static final TableName secondaryIdxTableName = TableName.valueOf(secondaryIdxTableNameString);
    private Table primaryTable;
    private Table secondaryIdxTable;

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
        LogManager.getLogger(DownstreamDataRippler.class).setLevel(Level.TRACE);
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
     * @return
     */
    private Map<String, String> configureHBase(String destination_table,
                                               String sec_idx_table,
                                               String src_cf,
                                               String trgt_cf,
                                               String sec_idx_cf,
                                               String amq_addr) {
        Map<String, String> kvs = new HashMap<>();
        kvs.put("destination_table", destination_table);
        kvs.put("source_column_family", src_cf);
        kvs.put("secondary_index_table", sec_idx_table);
        kvs.put("target_column_family", trgt_cf);
        kvs.put("secondary_index_cf", sec_idx_cf);
        kvs.put("amq_address", amq_addr);
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
        testingUtility.getConfiguration().setInt("hbase.client.scanner.timeout.period", 10);
        testingUtility.getConfiguration().setInt("hbase.client.retries.number", 1);
        testingUtility.getConfiguration().setInt("hbase.client.pause", 1);
        testingUtility.getConfiguration().setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);

        // Start up the testing cluster. No mocking around here!
        testingUtility.startMiniCluster();

        HBaseAdmin hbAdmin = testingUtility.getHBaseAdmin();

        System.out.println("Create secondary index");
        HTableDescriptor secIdxTableDesc = new HTableDescriptor(secondaryIdxTableName);
        secIdxTableDesc.addFamily(new HColumnDescriptor("a"));
        hbAdmin.createTable(secIdxTableDesc);

        System.out.println("Creating genome table");
        HTableDescriptor primaryTableDesc = new HTableDescriptor(primaryTableName);
        // Load the coprocessor on the primary table
        primaryTableDesc.addCoprocessor(DownstreamDataRippler.class.getName(), null, Coprocessor.PRIORITY_USER, kvs);
        primaryTableDesc.addFamily(new HColumnDescriptor("e"));
        hbAdmin.createTable(primaryTableDesc);

        secondaryIdxTable = testingUtility.getConnection().getTable(secondaryIdxTableName);
        primaryTable = testingUtility.getConnection().getTable(primaryTableName);
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
            brokerOptions.setConfigProperty("qpid.amqp_port", "5672");
            brokerOptions.setConfigProperty("qpid.pass_file", getResourcePath(passwordFileName));
            brokerOptions.setConfigProperty("qpid.work_dir", Files.createTempDir().getAbsolutePath());
            brokerOptions.setInitialConfigurationLocation(getResourcePath(configFileName));

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
    public void postPutHappyCase() throws Exception {

        Map<String, String> kvs = configureHBase("default", secondaryIdxTableNameString, "e", "eg", "a", amq_default_address);
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
        factory.setUri("amqp://guest:guest@localhost:5672/default");
        com.rabbitmq.client.Connection conn = factory.newConnection();
        com.rabbitmq.client.Channel channel = conn.createChannel();
        channel.basicConsume("default", false, new com.rabbitmq.client.DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       com.rabbitmq.client.Envelope envelope,
                                       com.rabbitmq.client.AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException {
                String routingKey = envelope.getRoutingKey();

                Assert.assertEquals("Routing key should be rowkey", "EFG1", routingKey);
                String contentType = properties.getContentType();
                Assert.assertEquals("Content type should be preserved", "application/json", contentType);
                long deliveryTag = envelope.getDeliveryTag();

                JSONObject jo = new JSONObject(body);
                String column_family = (String)jo.get("column_family");
                Assert.assertEquals("Column family should be preserved in the message body", "e", column_family);

                String column_value = (String)jo.get("column_value");
                Assert.assertEquals("Column value should be preserved in the message body", "some_value", column_value);

                channel.basicAck(deliveryTag, false);
            }
        });

    }
}
