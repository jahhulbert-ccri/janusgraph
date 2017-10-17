package org.janusgraph.diskstorage.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;

/**
 * Store Manage for Apache Accumulo
 *
 * @author Andrew Hulbert <jahhulbert@gmail.com>
 */
public class AccumuloStoreManager extends DistributedStoreManager implements KeyColumnValueStore {
    private static final Logger logger = LoggerFactory.getLogger(AccumuloStoreManager.class);

    public static final ConfigNamespace ACCUMULO_NS =
            new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "accumulo", "Accumulo storage options");

    public static final ConfigOption<String> ACCUMULO_TABLE =
            new ConfigOption<>(ACCUMULO_NS, "table",
                    "The namespace and table JanusGraph will use",
                    ConfigOption.Type.LOCAL, "janusgraph.janusgraph");

    public static final ConfigOption<String> ACCUMULO_INSTANCE =
            new ConfigOption<>(ACCUMULO_NS, "instance",
                    "instance name",
                    ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigOption<String> ACCUMULO_ZOOKEEPERS =
            new ConfigOption<>(ACCUMULO_NS, "zookeepers",
                    "zookeepers - comma separated host:port",
                    ConfigOption.Type.MASKABLE, String.class);
    
    public static final ConfigOption<String> ACCUMULO_USER =
            new ConfigOption<>(ACCUMULO_NS, "user",
                    "username",
                    ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigOption<String> ACCUMULO_PASSWORD=
            new ConfigOption<>(ACCUMULO_NS, "password",
                    "password",
                    ConfigOption.Type.MASKABLE, String.class);

    public static final int PORT_DEFAULT = 9160;

    public static final TimestampProviders PREFERRED_TIMESTAMPS = TimestampProviders.MILLI;
    
    private final String tableName;
    private final Connector connector;
    
    public AccumuloStoreManager(org.janusgraph.diskstorage.configuration.Configuration config) throws BackendException {
        super(config, PORT_DEFAULT);

        this.tableName = config.get(ACCUMULO_TABLE);

        final String instanceName = config.get(ACCUMULO_INSTANCE);
        final String zookeepers = config.get(ACCUMULO_ZOOKEEPERS);
        final String user = config.get(ACCUMULO_USER);
        final String password = config.get(ACCUMULO_PASSWORD);
        try {
            this.connector = new ZooKeeperInstance(instanceName, zookeepers).getConnector(user, new PasswordToken(password));
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new TemporaryBackendException("Error connecting to instance " + instanceName + "with zookeepers " +zookeepers, e);
        }



    }

    @java.lang.Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        return null;
    }

    @java.lang.Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        return null;
    }

    @java.lang.Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        return null;
    }

    @java.lang.Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {

    }

    @java.lang.Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {

    }

    @java.lang.Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        return null;
    }

    @java.lang.Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        return null;
    }

    @java.lang.Override
    public void close() throws BackendException {

    }

    @java.lang.Override
    public void clearStorage() throws BackendException {
        try {
            if (this.storageConfig.get(DROP_ON_CLEAR)) {
                connector.tableOperations().delete(tableName);
            } else {
                throw new UnsupportedOperationException("Need to implement deletemany");
                //connector.tableOperations().deleteRows();
            }
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            throw new TemporaryBackendException("Unable to delete table " + tableName, e)
        }
    }

    @java.lang.Override
    public boolean exists() throws BackendException {
        return false;
    }

    @java.lang.Override
    public StoreFeatures getFeatures() {

        Configuration c = GraphDatabaseConfiguration.buildGraphConfiguration();

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder()
                .orderedScan(true)
                .unorderedScan(true)
                .batchMutation(true)
                .multiQuery(true)
                .distributed(true)
                .keyOrdered(true)
                .storeTTL(true)
                .cellTTL(true)
                .timestamps(true)
                .preferredTimestamps(PREFERRED_TIMESTAMPS)
                .optimisticLocking(true)
                .keyConsistent(c);

        try {
            fb.localKeyPartition(getDeployment() == Deployment.LOCAL);
        } catch (Exception e) {
            logger.warn("Unexpected exception during getDeployment()", e);
        }

        return fb.build();
    }

    @java.lang.Override
    public String getName() {
        return tableName;
    }

    @java.lang.Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        return null;
    }

    @java.lang.Override
    public DistributedStoreManager.Deployment getDeployment() {
        return Deployment.REMOTE;
    }
}
