package net.nzcorp.hbase.mq_data_rippler;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

/**
 *
 */
@SuppressWarnings("unused")
public class MQDataRippler extends BaseRegionObserver {

    private Connection conn;
    private ConnectionFactory factory;
    private String destinationTable;
    private String secCF;
    private String sourceColumn;
    private static final Log LOGGER = LogFactory.getLog(net.nzcorp.hbase.mq_data_rippler.MQDataRippler.class);
    private String mq_ip;


    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        /**
         * The CoprocessorEnvironment.getConfiguration() will return a
         * Hadoop Configuration element as described here:
         * https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/conf/Configuration.html
         *
         * The named arguments given after the last pipe separator when installing the coprocessor will be available on
         * the above configuration element
         */

        conn = ConnectionFactory.createConnection( env.getConfiguration() );

        mq_ip = env.getConfiguration().get("mq_ip");

        factory = new ConnectionFactory();
        factory.setHost(mq_ip);
        //factory.setConnectionTimeout();


        secCF = env.getConfiguration().get("secondary_idx_cf");
        sourceColumn = env.getConfiguration().get("source_column");
        LOGGER.info("Initializing secondary indexer with destination table "+destinationTable+" and looking for "+sourceColumn);
        LOGGER.info("Will append to key in secondary index with column family "+secCF);
    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> observerContext,
                        final Put put,
                        final WALEdit edit,
                        final Durability durability_enum)
            throws IOException
    {
        try {

	    final List<Cell> cells = put.get(Bytes.toBytes("e"), Bytes.toBytes(sourceColumn));
            if (cells.isEmpty()) {
                return;
            }
            Cell sourceColumnCell = cells.get(0);
            byte[] sourceValue = CellUtil.cloneValue(sourceColumnCell);
            byte[] sourceKey = put.getRow();

            if(sourceValue.length == 0) {
                LOGGER.error(String.format("The key to write an entry in %s was empty! It came from %s={%s:%s}", destinationTable, new String(sourceKey),"e", sourceColumn));
            } else {
                LOGGER.info( String.format( "Upserting key %s with column value %s:%s", new String(sourceValue), secCF, new String( sourceKey ) ));
                /* Create new rowkey if none exists or append to the existing rowkey, possibly overwriting the values
                 * that previously were there
                 */
            }
            Connection connection = null;
            try {
                connection = factory.newConnection();
            } catch (TimeoutException e) {
                LOGGER.error(String.format("Timeout while trying to connect to MQ@%s", mq_ip));
            }
            Channel channel = connection.createChannel();


        } catch (IllegalArgumentException ex) {
            LOGGER.fatal("During the postPut operation, something went horribly wrong", ex);
            //In order not to drop its marbles when the HBase server throws an IllegalArgumentException, we allow the
            // coprocessor to continue operating, thereby allowing the HBase RS to continue operating.
            // This exception is throws before the secondary index table is opened, so just move along
            return;

        }

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        conn.close();
    }
}
