package net.nzcorp.hbase.debug_coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

//remember to add the hbase dependencies to the pom file
@SuppressWarnings("unused")
public class DebugCoprocessor extends BaseRegionObserver {

    private Connection conn;
    private String full_debug;
    private String sourceCF;
    private boolean f_debug;
    private static final Log LOGGER = LogFactory.getLog(DebugCoprocessor.class);


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

        full_debug = env.getConfiguration().get("full_debug");
        f_debug = Boolean.parseBoolean( full_debug );

        sourceCF = env.getConfiguration().get("source_column_family");
        if( sourceCF == null )
        {
            LOGGER.fatal("source_column_family was not set!");
        }

        LOGGER.info( String.format( "Are we on FULL DEBUG? %s", f_debug));
    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> observerContext,
                        final Put put,
                        final WALEdit edit,
                        final Durability durability_enum)
            throws IOException
    {
        try {
            long startTime = System.nanoTime();

            Method meth = Mutation.class.getDeclaredMethod("getCellList", byte[].class);
            meth.setAccessible(true);
            List<Cell> list_of_cells = (List<Cell>) meth.invoke(put, sourceCF.getBytes());

            if (list_of_cells.isEmpty()) {
                LOGGER.info("No cells in this transaction");
                return;
            }

            LOGGER.info("Found "+Integer.toString(list_of_cells.size())+" cells in Put");

            if( f_debug )
            {
                for (Cell cell: list_of_cells) {
                    final byte[] rowKey = CellUtil.cloneRow(cell);
                    LOGGER.info(String.format("Found rowkey: %s", new String(rowKey)));
                }
            }

            long endTime = System.nanoTime();
            long elapsedTime = (endTime - startTime)/1000000;
            LOGGER.info( String.format( "Exiting postPut, took %d%n milliseconds", elapsedTime ));
        } catch (IllegalArgumentException ex) {
            LOGGER.fatal("During the postPut operation, something went horribly wrong", ex);
            throw new IllegalArgumentException(ex);
        } catch (NoSuchMethodException e) {
            LOGGER.fatal( e );
        } catch (InvocationTargetException e) {
            LOGGER.fatal( e );
        } catch (IllegalAccessException e) {
            LOGGER.fatal( e );
        }

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        conn.close();
    }
}
