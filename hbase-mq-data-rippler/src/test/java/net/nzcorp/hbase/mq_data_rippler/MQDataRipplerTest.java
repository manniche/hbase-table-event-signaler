package net.nzcorp.hbase.mq_data_rippler;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MQDataRipplerTest {

    private static final TableName primaryTableName = TableName.valueOf("assembly");
    private static final TableName otherTableName = TableName.valueOf("genome_index");
    private HTable primaryTable;
    private HTable otherTable;

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
        testingUtility.getConfiguration().setStrings("destination_table", "genome_index");
        testingUtility.getConfiguration().setStrings("source_column", "genome_accession_number");
        testingUtility.getConfiguration().setStrings("secondary_idx_cf", "a");

        // Get random port number
        testingUtility.getConfiguration().setInt("hbase.regionserver.port", HBaseTestingUtility.randomFreePort());
        testingUtility.getConfiguration().setInt("hbase.master.info.port", HBaseTestingUtility.randomFreePort());

        testingUtility.startMiniCluster();

        byte[][] ba = new byte[2][];
        ba[0] = "e".getBytes();
        ba[1] = "foo".getBytes();

        primaryTable = (HTable) testingUtility.createTable(primaryTableName, ba);

        otherTable = (HTable) testingUtility.createTable(otherTableName, "a");
    }

    @After
    public void after() throws Exception {
        testingUtility.deleteTable(primaryTableName);
        testingUtility.deleteTable(otherTableName);
        primaryTable = null;
        otherTable = null;
        testingUtility.shutdownMiniCluster();
        testingUtility = null;
    }


    /**
     * maybe: https://hbase.apache.org/testdevapidocs/src-html/org/apache/hadoop/hbase/coprocessor/TestRegionObserverStacking.ObserverB.html
     * https://github.com/sematext/HBaseHUT/blob/master/src/test/java/com/sematext/hbase/hut/cp/TestHBaseHutCps.java
     * https://qnalist.com/questions/136205/hbase-coprocessor-unit-testing
     * https://github.com/apache/hbase/blob/master/hbase-endpoint/src/test/java/org/apache/hadoop/hbase/coprocessor/TestRegionServerCoprocessorEndpoint.java
     * @throws Exception
     */
    @Test
    public void postPutHappyCase() throws Exception {

        Put tablePut = new Put("EFB1".getBytes());
        tablePut.addColumn( "e".getBytes(), "genome_accession_number".getBytes(), "EFG1".getBytes());
        //tablePut.add(CellUtil.createCell("e".getBytes(), "genome_accession_number".getBytes(), "EFG1".getBytes()));
        primaryTable.put(tablePut);
        primaryTable.flushCommits();

        Get get = new Get("EFG1".getBytes());
        Result result = otherTable.get(get);
        List<Cell> cells = result.listCells();
        Cell cell = cells.get(0);
        String ck = new String(CellUtil.cloneRow(cell));
        String cf = new String(CellUtil.cloneFamily(cell));
        String cq = new String(CellUtil.cloneQualifier(cell));

        Assert.assertEquals( "We expect the column family from the secondary index","a", cf);
        Assert.assertEquals( "We expect the column to contain the rowkey for a target table", "EFB1", cq);
        Assert.assertEquals( "We expect the rowkey in the secondary index to be the source rowkey", "EFG1", ck);
    }

    @Test
    public void postPutMultiplePuts() throws Exception {
        Put tablePut1 = new Put( "EFB1".getBytes() );
        tablePut1.addColumn("e".getBytes(), "genome_accession_number".getBytes(), "EFG2".getBytes());
        Put tablePut2 = new Put( "EFB2".getBytes() );
        tablePut2.addColumn("e".getBytes(), "genome_accession_number".getBytes(), "EFG2".getBytes());

        List<Put> puts = new ArrayList<Put>();
        puts.add(tablePut1);
        puts.add(tablePut2);

        primaryTable.put( puts );
        primaryTable.flushCommits();

        Get get = new Get("EFG2".getBytes());
        Result result = otherTable.get(get);
        List<Cell> cells = result.listCells();

        Assert.assertEquals("We should have received two columns on EFG2", 2, cells.size());

        Cell cell = cells.get(0);
        String ck = new String(CellUtil.cloneRow(cell));
        String cf = new String(CellUtil.cloneFamily(cell));
        String cq = new String(CellUtil.cloneQualifier(cell));

        Assert.assertEquals( "We expect the column family from the secondary index","a", cf);
        Assert.assertEquals( "We expect the column to contain the rowkey for a target table", "EFB1", cq);
        Assert.assertEquals( "We expect the rowkey in the secondary index to be the source rowkey", "EFG2", ck);

        cell = cells.get(1);
        ck = new String(CellUtil.cloneRow(cell));
        cf = new String(CellUtil.cloneFamily(cell));
        cq = new String(CellUtil.cloneQualifier(cell));

        Assert.assertEquals( "We expect the column family from the secondary index","a", cf);
        Assert.assertEquals( "We expect the column to contain the rowkey for a target table", "EFB2", cq);
        Assert.assertEquals( "We expect the rowkey in the secondary index to be the source rowkey", "EFG2", ck);
    }

    @Test
    public void postPutToDifferentCFDoesNotIndex() throws Exception{
        Put tablePut = new Put( "EFB1".getBytes() );
        tablePut.addColumn("foo".getBytes(), "genome_accession_number".getBytes(), "EFG3".getBytes());

        primaryTable.put( tablePut );
        primaryTable.flushCommits();

        Get get = new Get("EFG3".getBytes());
        Result result = otherTable.get(get);
        List<Cell> cells = result.listCells();

        Assert.assertNull("We should not have received any columns on EFG3", cells);
    }

    @Test
    public void handlingPutWithEmptySourceValue() throws Exception {
        Put tablePut = new Put("EFB1".getBytes());
        tablePut.addColumn( "e".getBytes(), "genome_accession_number".getBytes(), "".getBytes());
        primaryTable.put(tablePut);
        primaryTable.flushCommits();

        Get get = new Get("EFG1".getBytes());
        Result result = otherTable.get(get);
        Assert.assertNull("There should be no secondary index for EFG1", result.listCells());
    }

}
