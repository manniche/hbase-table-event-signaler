package com.nzcorp.hbase.data_rippler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.runner.RunWith;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ConnectionFactory.class,
                 HBaseConfiguration.class})
public class DownstreamDataRipplerTest {

    private static final TableName primaryTable = TableName.valueOf("genome");
    private static final TableName otherTable = TableName.valueOf("assembly");
    private static final byte[] family = new byte[] { 'e' };

    private static boolean[] completed = new boolean[1];


    @Test
    public void postPutHappyCase() throws Exception {
        PowerMockito.mockStatic(ConnectionFactory.class);

        HTableDescriptor primary = new HTableDescriptor(primaryTable);
        primary.addFamily(new HColumnDescriptor(family));
        // add our coprocessor
        primary.addCoprocessor(DownstreamDataRippler.class.getName());

        HTableDescriptor other = new HTableDescriptor(otherTable);
        other.addFamily(new HColumnDescriptor(family));


        //Admin admin = UTIL.getHBaseAdmin();
        //admin.createTable(primary);
        //admin.createTable(other);

        //Configuration conf = HBaseConfiguration.create();
        System.out.println("Getting conn");
        Connection conn = ConnectionFactory.createConnection();
        System.out.println("Got conn");
        Table table = conn.getTable(primaryTable);
        Put p = new Put(new byte[] { 'a' });
        p.addColumn(family, null, new byte[]{'a'});
        table.put(p);
        table.close();

        Table target = conn.getTable(otherTable);

        //assertTrue("Didn't complete update to target table!", completeCheck[0]);
        //assertEquals("Didn't find inserted row", 1, getKeyValueCount(target));
        target.close();
    }

}