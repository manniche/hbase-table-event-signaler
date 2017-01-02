package com.nzcorp.hbase.data_rippler;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.*;

import java.util.Collections;
import java.util.List;

public class DownstreamDataRipplerTest {

    private static final TableName primaryTableName = TableName.valueOf("genome");
    private static final TableName otherTableName = TableName.valueOf("assembly");
    private static final TableName secondaryIdxTableName = TableName.valueOf("genome_index");
    private HTable primaryTable;
    private HTable otherTable;
    private HTable secondaryIdxTable;

    private static final byte[] family = new byte[] { 'e' };

    private static boolean[] completed = new boolean[1];

    private HBaseTestingUtility testingUtility = new HBaseTestingUtility();

    @BeforeClass
    public static void beforeClass() {
        List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for (Logger logger : loggers) {
            logger.setLevel(Level.ERROR);
        }
        LogManager.getLogger(DownstreamDataRippler.class).setLevel(Level.TRACE);
    }

    @Before
    public void before() throws Exception {
	/*
	 * Load the coprocessor on the region
	 */
        testingUtility.getConfiguration().setStrings(
                CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
                DownstreamDataRippler.class.getName());
        testingUtility.getConfiguration().setStrings("destination_table", "assembly");
        testingUtility.getConfiguration().setStrings("secondary_index_table", "genome_index");
        testingUtility.getConfiguration().setStrings("source_column_family", "e");
        testingUtility.getConfiguration().setStrings("target_column_family", "eg");
        testingUtility.getConfiguration().setStrings("secondary_index_cf", "a");

        // Get random port number
        testingUtility.getConfiguration().setInt("hbase.regionserver.port", HBaseTestingUtility.randomFreePort());
        testingUtility.getConfiguration().setInt("hbase.master.info.port", HBaseTestingUtility.randomFreePort());


        testingUtility.startMiniCluster();
        System.out.println("Creating genome table");
        primaryTable = (HTable) testingUtility.createTable(primaryTableName, "e");
        System.out.println("Creating assembly table" );

        byte[][] ba = new byte[2][];
        ba[0] = "e".getBytes();
        ba[1] = "eg".getBytes();

        otherTable = (HTable) testingUtility.createTable(otherTableName, ba);
        System.out.println("Creating secondary index table");
        secondaryIdxTable = (HTable) testingUtility.createTable(secondaryIdxTableName, "a");
    }

    @After
    public void after() throws Exception {
        primaryTable = null;
        otherTable = null;
        testingUtility.shutdownMiniCluster();
        testingUtility = null;
    }


    @Test
    public void postPutHappyCase() throws Exception {

	//populate assembly table
	Put assIdx = new Put("EFB1".getBytes());
	assIdx.addColumn("e".getBytes(), "genome_accession_number".getBytes(), "EFG1".getBytes());
	otherTable.put(assIdx);
	otherTable.flushCommits();

	//simulate population of secondary index as a result of the above
	Put idxPut = new Put("EFG1".getBytes());
	idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
	secondaryIdxTable.put(idxPut);
	secondaryIdxTable.flushCommits();

        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn( "e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);
        primaryTable.flushCommits();

        Get get = new Get("EFB1".getBytes());
        Result result = otherTable.get(get);
        List<Cell> cells = result.listCells();
        Cell cell = cells.get(1);
        String ck = new String(CellUtil.cloneRow(cell));
        String cf = new String(CellUtil.cloneFamily(cell));
        String cq = new String(CellUtil.cloneQualifier(cell));
	String vl = new String(CellUtil.cloneValue(cell));

        Assert.assertEquals( "We expect the column family from the rippled table","eg", cf);
        Assert.assertEquals( "We expect the column to contain the column value from the target table", "some_key", cq);
        Assert.assertEquals( "We expect the rowkey in the rippled table to be a EFB1", "EFB1", ck);
	Assert.assertEquals( "We expect the value in the rippled table to contain the value form the parent", "some_value", vl);
    }

    @Test
    public void postPutGetInterpretation() throws Exception {

	//populate assembly table
	Put assIdx = new Put("EFB10L".getBytes());
	assIdx.addColumn("e".getBytes(), "genome_accession_number".getBytes(), "EFG1".getBytes());
	otherTable.put(assIdx);
	otherTable.flushCommits();

	//simulate population of secondary index as a result of the above
	Put idxPut = new Put("EFG1".getBytes());
	idxPut.addColumn("a".getBytes(), "EFB10L".getBytes(), "".getBytes());
	secondaryIdxTable.put(idxPut);
	secondaryIdxTable.flushCommits();

        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn( "e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);
        primaryTable.flushCommits();

        Get get = new Get("EFB10L".getBytes());
        Result result = otherTable.get(get);
        List<Cell> cells = result.listCells();
        Cell cell = cells.get(0);
        String ck = new String(CellUtil.cloneRow(cell));
        String cf = new String(CellUtil.cloneFamily(cell));
        String cq = new String(CellUtil.cloneQualifier(cell));
        String vl = new String(CellUtil.cloneValue(cell));

        Assert.assertEquals( "We expect the rowkey in the rippled table to be the source rowkey", "EFB10L", ck);
        Assert.assertEquals( "We expect the column family from the rippled table","e", cf);
        Assert.assertEquals( "We expect the column to contain the rowkey for a target table", "genome_accession_number", cq);
	Assert.assertEquals( "We expect the value in the rippled table to contain the value form the parent", "EFG1", vl);

	cell = cells.get(1);
        ck = new String(CellUtil.cloneRow(cell));
        cf = new String(CellUtil.cloneFamily(cell));
        cq = new String(CellUtil.cloneQualifier(cell));
	vl = new String(CellUtil.cloneValue(cell));

        Assert.assertEquals( "We expect the rowkey in the rippled table to be the source rowkey", "EFB10L", ck);
        Assert.assertEquals( "We expect the column family from the rippled table","eg", cf);
        Assert.assertEquals( "We expect the column to contain the rowkey for a target table", "some_key", cq);
	Assert.assertEquals( "We expect the value in the rippled table to contain the value form the parent", "some_value", vl);
    }
}
