package org.vanilladb.core.storage.index.ivf;

import static org.vanilladb.core.sql.Type.DOUBLE;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VECTOR;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.util.CoreProperties;
import org.vanilladb.core.storage.index.ivf.DataRecord;

public class IVFIndex extends Index {

    private static final String SCHEMA_ID = "i_id", SCHEMA_CID = "c_id", SCHEMA_VECTOR = "i_emb",
            SCHEMA_RID_BLOCK = "block", SCHEMA_RID_ID = "id";
    private static final int NUM_CLUSTERS;

    static {
        NUM_CLUSTERS = CoreProperties.getLoader().getPropertyAsInteger(IVFIndex.class.getName() + ".NUM_CLUSTERS", 200);
    }

    // private static String vecFieldName(int index) {
    // return SCHEMA_VECTOR_PREFIX + index;
    // }
    public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
        // copied from HashIndex.java
        // int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(schema(keyType));
        // return (totRecs / rpb) / NUM_CLUSTERS;
        return 0;
    }

    /**
     * Returns the schema of the index records.
     * 
     * @param fldType
     *                the type of the indexed field
     * 
     * @return the schema of the index records
     */
    private static Schema schema_centroid(SearchKeyType keyType) {
        Schema sch = new Schema();
        sch.addField(SCHEMA_CID, INTEGER);
        sch.addField(SCHEMA_VECTOR, keyType.get(0));
        // sch.addField(SCHEMA_RID_BLOCK, BIGINT);
        // sch.addField(SCHEMA_RID_ID, INTEGER);
        return sch;
    }

    private static Schema schema_cluster(SearchKeyType keyType) {
        Schema sch = new Schema();
        sch.addField(SCHEMA_ID, INTEGER);
        sch.addField(SCHEMA_VECTOR, keyType.get(0));
        sch.addField(SCHEMA_RID_BLOCK, BIGINT);
        sch.addField(SCHEMA_RID_ID, INTEGER);
        return sch;
    }

    // store key and record id
    private int clusterID;
    private List<Integer> clusterIDs;
    private SearchKey searchKey;
    private RecordFile rf;
    private boolean isBeforeFirsted;
    private String centroidTblname, clusterTblnamePrefix;
    private boolean tableSet = false;

    DistanceFn distFn_vec;

    public IVFIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
        super(ii, keyType, tx);
        this.distFn_vec = new EuclideanFn("i_emb");
        centroidTblname = "cluster_center";
        clusterTblnamePrefix = "cluster_";
    }

    @Override
    public void preLoadToMemory() {
        // for (int i = 0; i < numClusters; i++) {
        // String tblname = ii.indexName() + i + ".tbl";
        long size = fileSize(centroidTblname);
        BlockId blk;
        for (int j = 0; j < size; j++) {
            blk = new BlockId(centroidTblname, j);
            tx.bufferMgr().pin(blk);
        }
        // }
    }

    private VectorConstant extractVector(SearchKey key) {
        VectorConstant vec = (VectorConstant) key.get(0);
        // System.out.println("IVFIndex extractVector: " +
        // Arrays.toString(vec.asJavaVal()));
        return vec;
    }

    public class Pair<K, V> {
        private K key;
        public V value;

        public Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }
    }

    private List<Integer> searchKClosestCluster(int k, VectorConstant vec) {
        // System.out.println("searchClosestCluster: " + centroidTblname);
        List<Integer> kClosestClusters = new ArrayList<>();
        this.distFn_vec.setQueryVector(vec);
        PriorityQueue<Pair<Double, Integer>> maxHeap = new PriorityQueue<>(k,
                Comparator.comparingDouble(Pair<Double, Integer>::getKey).reversed());
        TableInfo ti = new TableInfo(centroidTblname, schema_centroid(keyType));
        // open centroid file
        this.rf = ti.open(tx, false);
        rf.beforeFirst();
        while (rf.next()) {
            Constant cid = rf.getVal(SCHEMA_CID);
            Constant centroid_vec = rf.getVal(SCHEMA_VECTOR);
            double distance = distFn_vec.distance((VectorConstant) centroid_vec);
            if (maxHeap.size() < k) {
                maxHeap.add(new Pair<>(distance, (int) cid.asJavaVal()));
            } else if (distance < maxHeap.peek().getKey()) {
                maxHeap.poll();
                maxHeap.add(new Pair<>(distance, (int) cid.asJavaVal()));
            }
        }
        rf.close();
        while (!maxHeap.isEmpty()) {
            kClosestClusters.add(maxHeap.poll().value);
        }
        return kClosestClusters;
    }

    private int searchClosestCluster(VectorConstant vec) {
        // System.out.println("searchClosestCluster: " + centroidTblname);
        double minDistance = Double.MAX_VALUE;
        this.distFn_vec.setQueryVector(vec);
        int closestClusterIndex = -1;
        TableInfo ti = new TableInfo(centroidTblname, schema_centroid(keyType));
        // open centroid file
        this.rf = ti.open(tx, false);
        rf.beforeFirst();
        while (rf.next()) {
            Constant cid = rf.getVal(SCHEMA_CID);
            Constant centroid_vec = rf.getVal(SCHEMA_VECTOR);
            double distance = distFn_vec.distance((VectorConstant) centroid_vec);
            if (distance < minDistance) {
                minDistance = distance;
                closestClusterIndex = (int) cid.asJavaVal();
            }
        }
        return closestClusterIndex;
    }

    @Override
    public void beforeFirst(SearchRange searchRange) {
        close();
        // support the range query only for this IVF index implementation
        if (!searchRange.isSingleValue())
            throw new UnsupportedOperationException();

        this.searchKey = searchRange.asSearchKey();
        VectorConstant queryVec = extractVector(searchKey);

        this.clusterIDs = searchKClosestCluster(2, queryVec);
        this.clusterID = 0;
        String tblname = this.clusterTblnamePrefix + this.clusterIDs.get(clusterID);
        TableInfo ti = new TableInfo(tblname, schema_cluster(keyType));
        this.rf = ti.open(tx, false);

        // this.clusterID = searchClosestCluster(queryVec);
        // String tblname = this.clusterTblnamePrefix + this.clusterID;
        // TableInfo ti = new TableInfo(tblname, schema_cluster(keyType));
        // this.rf = ti.open(tx, false);

        if (rf.fileSize() == 0)
            RecordFile.formatFileHeader(ti.fileName(), tx);
        rf.beforeFirst();

        isBeforeFirsted = true;
    }

    @Override
    public boolean next() {
        if (!isBeforeFirsted)
            throw new IllegalStateException("You must call beforeFirst() before iterating index '"
                    + ii.indexName() + "'");

        while (rf.next())
            return true;
        rf.close();

        if (clusterID < clusterIDs.size() - 1) {
            clusterID++;
            String tblname = this.clusterTblnamePrefix + this.clusterIDs.get(clusterID);
            TableInfo ti = new TableInfo(tblname, schema_cluster(keyType));
            this.rf = ti.open(tx, false);
            if (rf.fileSize() == 0)
                RecordFile.formatFileHeader(ti.fileName(), tx);
            rf.beforeFirst();

            if (rf.next()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RecordId getDataRecordId() {
        long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
        int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
        return new RecordId(new BlockId(dataFileName, blkNum), id);
    }

    public void setClusterTable(List<List<DataRecord>> cluster) {
        // centroid file
        TableInfo ti = new TableInfo(centroidTblname, schema_centroid(keyType));
        RecordFile centroidFile = ti.open(tx, true);
        RecordFile.formatFileHeader(ti.fileName(), tx);
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            if (cluster.get(i).size() == 0) {
                System.out.println("Cluster " + i + " is empty");
                continue;
            }
            VectorConstant vector = (VectorConstant) cluster.get(i).get(0).i_emb;
            centroidFile.insert();
            centroidFile.setVal(SCHEMA_CID, new IntegerConstant(i));
            centroidFile.setVal(SCHEMA_VECTOR, vector);
        }
        // cluster file
        for (int i = 0; i < NUM_CLUSTERS; ++i) {
            if (cluster.get(i).size() == 0) {
                System.out.println("Cluster " + i + " is empty");
                continue;
            }
            System.out.println("Setting table for cluster_" + i + " with size: " + cluster.get(i).size());
            String tblname = clusterTblnamePrefix + i;
            TableInfo cluster_ti = new TableInfo(tblname, schema_cluster(keyType));
            RecordFile cluster_rf = cluster_ti.open(tx, true);
            if (cluster_rf.fileSize() == 0)
                RecordFile.formatFileHeader(cluster_ti.fileName(), tx);
            cluster_rf.beforeFirst();
            for (int j = 0; j < cluster.get(i).size(); j++) {
                DataRecord rec = cluster.get(i).get(j);
                cluster_rf.insert();
                VectorConstant vector = (VectorConstant) rec.i_emb;
                cluster_rf.setVal(SCHEMA_VECTOR, vector);
                cluster_rf.setVal(SCHEMA_RID_BLOCK, rec.blockNum);
                cluster_rf.setVal(SCHEMA_RID_ID, rec.recordId);
            }
        }
        tableSet = true;
    }

    @Override
    public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        if (!tableSet)
            return;
        // search the position
        beforeFirst(new SearchRange(key));

        // log the logical operation starts
        // if (doLogicalLogging)
        // tx.recoveryMgr().logLogicalStart();

        // insert the data
        rf.insert();
        rf.setVal(SCHEMA_VECTOR, key.get(0));
        rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block()
                .number()));
        rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));

        // log the logical operation ends
        // if (doLogicalLogging)
        // tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
        // dataRecordId.block().number(), dataRecordId.id());
    }

    @Override
    public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        if (!tableSet)
            return;
        // search the position
        beforeFirst(new SearchRange(key));

        // log the logical operation starts
        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();

        // delete the specified entry
        while (next()) {
            if (getDataRecordId().equals(dataRecordId)) {
                rf.delete();
                return;
            }
        }

        // log the logical operation ends
        if (doLogicalLogging)
            tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key,
                    dataRecordId.block().number(), dataRecordId.id());
    }

    /**
     * Closes the index by closing the current table scan.
     * 
     * @see Index#close()
     */
    @Override
    public void close() {
        if (rf != null)
            rf.close();
    }

    private long fileSize(String fileName) {
        tx.concurrencyMgr().readFile(fileName);
        return VanillaDb.fileMgr().size(fileName);
    }
}
