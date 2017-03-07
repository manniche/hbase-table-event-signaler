package net.nzcorp.hbase.debug_coprocessor;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.*;

import java.util.*;

public class DebugCoprocessorTest {

    private static final String primaryTableNameString = "genome";
    private static final TableName primaryTableName = TableName.valueOf(primaryTableNameString);
    private Table primaryTable;
    private HBaseTestingUtility testingUtility = new HBaseTestingUtility();

    @BeforeClass
    public static void beforeClass() {
        List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for (Logger logger : loggers) {
            logger.setLevel(Level.OFF);
        }
        LogManager.getLogger(DebugCoprocessor.class).setLevel(Level.TRACE);

    }

    @Before
    public void before() throws Exception {
        // Get random port number
        testingUtility.getConfiguration().setInt("hbase.regionserver.port", HBaseTestingUtility.randomFreePort());
        testingUtility.getConfiguration().setInt("hbase.master.info.port", HBaseTestingUtility.randomFreePort());
        testingUtility.getConfiguration().setInt("hbase.client.scanner.timeout.period", 1000);
        testingUtility.getConfiguration().setInt("hbase.client.retries.number", 2);
        testingUtility.getConfiguration().setInt("hbase.client.pause", 1);
        testingUtility.getConfiguration().setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);
        testingUtility.startMiniCluster();

        HBaseAdmin hbAdmin = testingUtility.getHBaseAdmin();

        HTableDescriptor primaryTableDesc = new HTableDescriptor(primaryTableName);

        primaryTableDesc.addFamily(new HColumnDescriptor("e"));

        // Load the coprocessor on the primary table
        Map<String, String> kvs = new HashMap<>();
        kvs.put("source_column_family", "e");
        kvs.put("full_debug", "true");
        primaryTableDesc.addCoprocessor(DebugCoprocessor.class.getName(), null, Coprocessor.PRIORITY_USER, kvs);
        hbAdmin.createTable(primaryTableDesc);

        primaryTable = testingUtility.getConnection().getTable(primaryTableName);
    }

    @After
    public void after() throws Exception {
        testingUtility.deleteTableIfAny(primaryTableName);
        primaryTable = null;
        testingUtility.shutdownMiniCluster();
        testingUtility = null;
    }

    @Test
    public void testDeleteSingleColumn() throws Exception {
        Put p = new Put("EFG1".getBytes());
        p.addColumn("e".getBytes(), "foo1".getBytes(), "bar_val1".getBytes());
        p.addColumn("e".getBytes(), "foo2".getBytes(), "bar_val2".getBytes());
        primaryTable.put(p);

        Delete d = new Delete("EFG1".getBytes());
        d.addColumn("e".getBytes(), "foo1".getBytes());
        primaryTable.delete(d);
   }

    @Test
    public void testDeleteRow() throws Exception {
        Put p = new Put("EFG1".getBytes());
        p.addColumn("e".getBytes(), "foo1".getBytes(), "bar_val1".getBytes());
        p.addColumn("e".getBytes(), "foo2".getBytes(), "bar_val2".getBytes());
        primaryTable.put(p);

        Delete d = new Delete("EFG1".getBytes());
        primaryTable.delete(d);
    }
}
