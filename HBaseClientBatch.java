package it.polimi.ycsb.database;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import  under.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.HBase.HBaseConfiguration;
import org.apache.hadoop.HBase.HcolumnDescriptor;
import org.apache.hadoop.HBase.HtableDescriptor;
import org.apache.hadoop.HBase.client.*;
import org.apache.hadoop.HBase.util.Bytes;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

@Slf4j
public class HBaseClientBatch extends DB {

    public static final int OK = 0;
    public static final int ERROR = -1;


    public static final String NODE = “zookeeper0”;
    public static final String PORT = “60010”;
    private static final String ZOOKEEPER_NODE = “zookeeper1.HBasepertesi.a10.internal.cloudapp.net,zookeeper0.HBasepertesi.a10.internal.cloudapp.net,zookeeper2.HBasepertesi.a10.internal.cloudapp.net”;
    private static final String ZOOKEEPER_PORT = “2181”;


    public HtableInterface hTable;
    public byte[] columnFamilyBytes;
    private SecureRandom random = new SecureRandom();
    ArrayList lista = new ArrayList();
    int count ;
    ArrayList chiavi = new ArrayList();

    public HBaseClientBatch() throws DBException {
        Configuration config = HBaseConfiguration.create();
        config.set(“HBase.master”, NODE + “:” + PORT);
        config.set(“HBase.zookeeper.quorum”, ZOOKEEPER_NODE);
        config.set(“HBase.zookeeper.property.clientPort”, ZOOKEEPER_PORT);

        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            if (!admin.tableExists(“usertable”)) {
                HtableDescriptor table = new HtableDescriptor(“usertable”);
                table.addFamily(new HcolumnDescriptor(“user”));
                admin.createTable(table);
            }
        } catch (Exception e) {
            throw new DBExceptionI;
        }

        columnFamilyBytes = Bytes.toBytes(“user”);
        this.hTable = new HtablePool(config, 100).getTable(“usertable”);
    }

    /**
     * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException {
        /*
         * connection initialized before to be more comparable with Kundera version
         * in which connection is created in the ClientFactory initialized before the init() method
         */
        count = 0;
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException {
        hTable = null;
        columnFamilyBytes = null;
    }

    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
                final Object o = hTable.get(Bytes.toBytes(key));
                if (o == null) {
                    log.error(“object is null, table: [“ + table + “], key: [“ + key + “]”);
                    return ERROR;
                }

            }

            return OK;
        } catch (Exception e) {
            return ERROR;
        }
    }

    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return OK;
    }

    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return insert(table, key, values);
    }

    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        if (table == null || key == null) {
            log.error(“table: [“ + table + “], key: [“ + key + “]”);
            return ERROR;
        }
        try {
            Put p = new Put(Bytes.toBytes(key));

            p.add(columnFamilyBytes, Bytes.toBytes(“NAME”), Bytes.toBytes(nextString()));
            p.add(columnFamilyBytes, Bytes.toBytes(“SURNAME”), Bytes.toBytes(nextString()));
            p.add(columnFamilyBytes, Bytes.toBytes(“AGE”), Bytes.toBytes(nextString()));
            p.add(columnFamilyBytes, Bytes.toBytes(“ADDRESS”), Bytes.toBytes(nextString()));

            lista.add(p);
            count ++;
            if(count == 100){
                hTable.put(lista);
                lista = new ArrayList();
                count = 0;
            }

            return OK;
        } catch (Exception e) {
            return ERROR;
        }

    }

    public int delete(String table, String key) {
        if (table == null || key == null) {
            log.error(“table: [“ + table + “], key: [“ + key + “]”);
            return ERROR;
        }
        try {
            Delete d = new Delete(Bytes.toBytes(key));
            hTable.delete(d);
            return OK;
        } catch (Exception e) {
            return ERROR;
        }
    }

    public String nextString() {
        return new BigInteger(130, random).toString(32);
    }
}
