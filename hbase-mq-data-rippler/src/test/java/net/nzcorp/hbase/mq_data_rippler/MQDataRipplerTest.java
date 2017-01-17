package net.nzcorp.hbase.mq_data_rippler;

import com.rabbitmq.client.Channel;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.*;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class MQDataRipplerTest {

    private static final TableName primaryTableName = TableName.valueOf("assembly");
    private org.apache.hadoop.hbase.client.HTable primaryTable;

    private HBaseTestingUtility testingUtility = new HBaseTestingUtility();

    @BeforeClass
    public static void beforeClass() {
        List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for (Logger logger : loggers) {
            logger.setLevel(Level.OFF);
        }
        LogManager.getLogger(MQDataRippler.class).setLevel(Level.TRACE);
    }

    @Before
    public void before() throws Exception {
        /*
         * Load the coprocessor on the region
         */
        testingUtility.getConfiguration().setStrings(
                CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
                MQDataRippler.class.getName());

        /*
         * Set configuration values (emulating the alter_async command for installing coprocessors
         */
        testingUtility.getConfiguration().setStrings("destination_table", "genome_index");
        testingUtility.getConfiguration().setStrings("source_column", "genome_accession_number");
        testingUtility.getConfiguration().setStrings("secondary_idx_cf", "a");

        // Get random port number for hbase master and RS
        testingUtility.getConfiguration().setInt("hbase.regionserver.port", HBaseTestingUtility.randomFreePort());
        testingUtility.getConfiguration().setInt("hbase.master.info.port", HBaseTestingUtility.randomFreePort());

        testingUtility.startMiniCluster();

        /*
         * Create table with column families
         */
        byte[][] ba = new byte[2][];
        ba[0] = "e".getBytes();
        ba[1] = "foo".getBytes();

        primaryTable = (org.apache.hadoop.hbase.client.HTable) testingUtility.createTable(primaryTableName, ba);
    }

    @After
    public void after() throws Exception {
        testingUtility.deleteTable(primaryTableName);
        primaryTable = null;
        testingUtility.shutdownMiniCluster();
        testingUtility = null;
    }

    @Test
    public void postPutHappyCase() throws Exception {
        ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
        Connection mockConnection = mock(Connection.class);
        Channel mockChannel = mock(Channel.class);

        when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
        when(mockConnection.isOpen()).thenReturn(true);
        when(mockConnection.createChannel()).thenReturn(mockChannel);

        when(mockChannel.isOpen()).thenReturn(true);
    }

}
