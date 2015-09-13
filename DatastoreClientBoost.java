package it.polimi.ycsb.database;

import com.google.appengine.api.datastore.*;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.appengine.tools.remoteapi.RemoteApiOptions;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import lombok.extern.slf4j.Slf4j;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;


@Slf4j
public class DatastoreClientBoost extends DB {

    private static final int OK = 0;
    private static final int ERROR = -1;
    private RemoteApiInstaller installer;
    private DatastoreService datastore;
    private SecureRandom random = new SecureRandom();
int count;
List<Entity> lista = new ArrayList<Entity>();
List<Key> chiavi = new ArrayList<Key>();
    /**
     * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException {
        try {
		count = 0;
            String url = getProperties().getProperty("url");
            String stringPort = getProperties().getProperty("port");
            int port = 443;
            if (stringPort != null) {
                port = Integer.valueOf(stringPort);
            }
            String username = getProperties().getProperty("username");
            String password = getProperties().getProperty("password");

            RemoteApiOptions options = new RemoteApiOptions().server(url, port).credentials(username, password);
            this.installer = new RemoteApiInstaller();
            this.installer.install(options);

            DatastoreServiceConfig config = DatastoreServiceConfig.Builder.withDefaults();
            datastore = DatastoreServiceFactory.getDatastoreService(config);
        } catch (Exception e) {
            throw new DBException(e);
        }
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException {
        if (installer != null) {
            installer.uninstall();
        }
        datastore = null;
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {


        if (table == null || key == null) {
            log.error("table: [" + table + "], key: [" + key + "]");
            return ERROR;
        }
        try {

	    final Object o = datastore.get(KeyFactory.createKey(table,key));

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
            Entity gaeEntity = new Entity(table, key);

            gaeEntity.setProperty("NAME", nextString());
            gaeEntity.setProperty("SURNAME", nextString());
            gaeEntity.setProperty("AGE", nextString());
            gaeEntity.setProperty("ADDRESS", nextString());
	lista.add(gaeEntity);
	count ++;
	if(count == 100){
	
            datastore.put(lista);
lista = new ArrayList<Entity>();
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
        if (table == null || key == null) {
            log.error("table: [" + table + "], key: [" + key + "]");
            return ERROR;
        }
        try {
            datastore.delete(KeyFactory.createKey(table, key));
            return OK;
        } catch (Exception e) {
            log.error(e.getMessage());
            return ERROR;
        }
    }

    public String nextString() {
        return new BigInteger(130, random).toString(32);
    }
}
