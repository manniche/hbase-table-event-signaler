package com.nzcorp.hbase.data_rippler;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DownstreamDataRipplerTest {

    private static final String amq_default_address = "localhost:5672";
    private static final String otherTableNameString = "assembly";
    private static final String secondaryIdxTableNameString = "genome_index";
    private static final TableName primaryTableName = TableName.valueOf("genome");
    private static final TableName otherTableName = TableName.valueOf(otherTableNameString);
    private static final TableName secondaryIdxTableName = TableName.valueOf(secondaryIdxTableNameString);
    private Table primaryTable;
    private Table otherTable;
    private Table secondaryIdxTable;

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

    private Map<String, String> configureHBase(String dest_table,
                                String sec_idx_table,
                                String src_cf,
                                String trgt_cf,
                                String sec_idx_cf,
                                String amq_addr) {
        Map<String, String> kvs = new HashMap<String, String>();
        kvs.put("destination_table", dest_table);
        kvs.put("source_column_family", src_cf);
        kvs.put("secondary_index_table", sec_idx_table);
        kvs.put("target_column_family", trgt_cf);
        kvs.put("secondary_index_cf", sec_idx_cf);
        kvs.put("amq_address", amq_addr);
        return kvs;
    }

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

        System.out.println("Creating assembly table");
        HTableDescriptor downstreamTableDesc = new HTableDescriptor(otherTableName);
        downstreamTableDesc.addFamily(new HColumnDescriptor("e"));
        downstreamTableDesc.addFamily(new HColumnDescriptor("eg"));
        hbAdmin.createTable(downstreamTableDesc);

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

        otherTable = testingUtility.getConnection().getTable(otherTableName);
        secondaryIdxTable = testingUtility.getConnection().getTable(secondaryIdxTableName);
        primaryTable = testingUtility.getConnection().getTable(primaryTableName);
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
        Map<String, String> kvs = configureHBase(otherTableNameString, secondaryIdxTableNameString, "e", "eg", "a", amq_default_address);
        setupHBase(kvs);

        //populate assembly table
        Put assIdx = new Put("EFB1".getBytes());
        assIdx.addColumn("e".getBytes(), "genome_accession_number".getBytes(), "EFG1".getBytes());
        otherTable.put(assIdx);
        //otherTable.flushCommits();

        //simulate population of secondary index as a result of the above
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB1".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);
        //secondaryIdxTable.flushCommits();

        // Add a column to the primary table, which should trigger a data ripple to the downstream table
        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn( "e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);
        //primaryTable.flushCommits();

        // now we should be able to retrieve the rippled values from the downstream table
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
        Map<String, String> kvs = configureHBase(otherTableNameString, secondaryIdxTableNameString, "e", "eg", "a", amq_default_address);

        setupHBase(kvs);

        //populate assembly table
        Put assIdx = new Put("EFB10L".getBytes());
        assIdx.addColumn("e".getBytes(), "genome_accession_number".getBytes(), "EFG1".getBytes());
        otherTable.put(assIdx);
        //otherTable.flushCommits();

        //simulate population of secondary index as a result of the above
        Put idxPut = new Put("EFG1".getBytes());
        idxPut.addColumn("a".getBytes(), "EFB10L".getBytes(), "".getBytes());
        secondaryIdxTable.put(idxPut);
        //secondaryIdxTable.flushCommits();

        Put tablePut = new Put("EFG1".getBytes());
        tablePut.addColumn( "e".getBytes(), "some_key".getBytes(), "some_value".getBytes());
        primaryTable.put(tablePut);
        //primaryTable.flushCommits();

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
