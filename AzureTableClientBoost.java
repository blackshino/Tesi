package it.polimi.ycsb.database;


import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import it.polimi.kundera.client.azuretable.DynamicEntity;
import it.polimi.kundera.client.azuretable.DynamicEntityTwo;
import lombok.extern.slf4j.Slf4j;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.table.*;
import com.microsoft.azure.storage.table.TableQuery;

import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.EntityProperty;
import it.polimi.ycsb.entities.*;


import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;


@Slf4j
public class AzureTableClientBoost extends DB {

    private static final int OK = 0;
    private static final int ERROR = -1;
    private CloudTableClient tableClient;
    private SecureRandom random = new SecureRandom();
    int count = 1;
    TableBatchOperation batchOperation = new TableBatchOperation();
CloudTable tablex;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
     */

    public void init() throws DBException {
        count = 0;
 try {
            String storageConnectionString;
            String useEmulator = getProperties().getProperty("emulator");
            if (useEmulator != null && useEmulator.equalsIgnoreCase("true")) {
                storageConnectionString = "UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1";
            } else {
                String accountName = getProperties().getProperty("account.name");
                String accountKey = getProperties().getProperty("account.key");
                String protocol = getProperties().getProperty("protocol");
                if (protocol == null || protocol.isEmpty()) {
                    protocol = "https";
                }
                storageConnectionString = "DefaultEndpointsProtocol=" + protocol + ";AccountName=" + accountName + ";AccountKey=" + accountKey;

            }

            tableClient = CloudStorageAccount.parse(storageConnectionString).createCloudTableClient();
            tablex = tableClient.getTableReference("provax");
            tablex.createIfNotExists();
        } catch (Exception e) {

            throw new DBException(e);
        }
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one DB instance per client thread.
     */

    public void cleanup() throws DBException {
        tableClient = null;
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        if (table == null || key == null) {
            log.error("table: [" + table + "], key: [" + key + "]");
            return ERROR;
        }
        try {
        TableOperation retrieveOperation = TableOperation.retrieve(table, key, DynamicEntityTwo.class);

  	final Object o = tablex.execute(retrieveOperation);

            if (o == null) {
                log.error("object is null, table: [" + table + "], key: [" + key + "]");
                return ERROR;
            }

            return OK;

        } catch (Exception e) {
            log.error(e.getMessage());
            return ERROR;
        }



    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return OK;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return insert(table, key, values);
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        if (table == null || key == null) {
            log.error("table: [" + table + "], key: [" + key + "]");
            return ERROR;
        }
        try {
           DynamicEntityTwo tableEntity = new DynamicEntityTwo(table, key);
	    tableEntity.setProperty("NAME", nextString());
            tableEntity.setProperty("SURNAME", nextString());
            tableEntity.setProperty("AGE", nextString());
          tableEntity.setProperty("ADDRESS", nextString());
	TableOperation insertOperation = TableOperation.insertOrReplace(tableEntity);

batchOperation.add(insertOperation);
            count++;
            if(count == 100) {

		 tablex.execute(batchOperation);
		batchOperation = new TableBatchOperation();
                count = 0;
            }

            return OK;
        } catch (Exception e) {
            log.error(e.getMessage());
            return ERROR;
        }
    }

    @Override
    public int delete(String table, String key) {

return ERROR;

    }

    public EntityProperty nextString() {
        return new EntityProperty(new BigInteger(130, random).toString(32));
    }
}
