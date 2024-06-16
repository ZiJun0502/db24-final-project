package org.vanilladb.core.storage.index.ivf;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import javax.xml.crypto.Data;

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

public class IVFIndex extends Index {

    private static final String SCHEMA_ID = "i_id", SCHEMA_CID = "c_id", SCHEMA_VECTOR = "i_emb",
            SCHEMA_RID_BLOCK = "block", SCHEMA_RID_ID = "id";
    private static final int NUM_CLUSTERS;
    public static List<DataRecord> data;

    static {
        NUM_CLUSTERS = CoreProperties.getLoader().getPropertyAsInteger(IVFIndex.class.getName() + ".NUM_CLUSTERS", 200);
        data = new ArrayList<>();
    }
    public static int getNumClusters(){
        return NUM_CLUSTERS;
    }

    public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
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
    private int clusterLevel1;
    private List<Integer> clusterIDs;
    private List<Integer> clusterLevel2;
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
        clusterTblnamePrefix = "cluster";
    }

    @Override
    public void preLoadToMemory() {
        // for (int i = 0; i < numClusters; i++) {
        String tblname = centroidTblname + ".tbl";

        long size = fileSize(tblname);
        System.out.println("Pre-load "+tblname+" to memory with size: " + size);
        BlockId blk;
        for (int j = 0; j < size; j++) {
            blk = new BlockId(tblname, j);
            tx.bufferMgr().pin(blk);
        }
        for (int i = 0 ; i < 10 ; i++) {
            String tblnameLevel2 = centroidTblname + "_" + i + ".tbl";
            System.out.println("Pre-load "+tblnameLevel2+" to memory with size: " + size);
            size = fileSize(tblnameLevel2);
            for (int j = 0; j < size; j++) {
                blk = new BlockId(tblnameLevel2, j);
                tx.bufferMgr().pin(blk);
            }
        }
    }

    private VectorConstant extractVector(SearchKey key) {
        VectorConstant vec = (VectorConstant) key.get(0);
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

    private List<Integer> searchKClosestCluster(int k, VectorConstant vec, String filenamePosfix) {
        List<Integer> kClosestClusters = new ArrayList<>();
        this.distFn_vec.setQueryVector(vec);
        PriorityQueue<Pair<Double, Integer>> maxHeap = new PriorityQueue<>(k,
                Comparator.comparingDouble(Pair<Double, Integer>::getKey).reversed());
        TableInfo ti = new TableInfo(centroidTblname + filenamePosfix, schema_centroid(keyType));
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
    private int searchClosestCluster(VectorConstant vec, String filenamePosfix) {
        // System.out.println("searchClosestCluster: " + centroidTblname);
        double minDistance = Double.MAX_VALUE;
        this.distFn_vec.setQueryVector(vec);
        int closestClusterIndex = -1;
        TableInfo ti = new TableInfo(centroidTblname, schema_centroid(keyType));
        // System.out.println("LEVEL1 Search table: " + centroidTblname);
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
    private void searchKClosestCluster(int k, VectorConstant vec) {
        int cLevel1 = searchClosestCluster(vec, "");
        // System.out.println("Closest level1 centroid: " + cLevel1);
        List<Integer> cLevel2 = searchKClosestCluster(k, vec, "_" + cLevel1);
        // for (int i : cLevel2) {
            // System.out.println("    Closest level2 centroids: " + i);
        // }
        this.clusterLevel1 = cLevel1;
        this.clusterLevel2 = cLevel2;
    }

    @Override
    public void beforeFirst(SearchRange searchRange) {
        close();
        // support the range query only for this IVF index implementation
        if (!searchRange.isSingleValue())
            throw new UnsupportedOperationException();

        this.searchKey = searchRange.asSearchKey();
        VectorConstant queryVec = extractVector(searchKey);

        searchKClosestCluster(2, queryVec);
        this.clusterID = 0;
        String tblname = this.clusterTblnamePrefix 
                    + "_" + this.clusterLevel1 
                    + "_" + this.clusterLevel2.get(clusterID);
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

        if (clusterID < this.clusterLevel2.size() - 1) {
            clusterID++;
            String tblname = this.clusterTblnamePrefix 
                        + "_" + this.clusterLevel1 
                        + "_" + this.clusterLevel2.get(clusterID);
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
    public void setClusterTable(List<DataRecord> centroids, List<List<DataRecord>> cluster, String filenamePosfix) {
        // centroid file

        TableInfo ti = new TableInfo(centroidTblname + filenamePosfix, schema_centroid(keyType));
        RecordFile centroidFile = ti.open(tx, true);
        RecordFile.formatFileHeader(ti.fileName(), tx);
        for (int i = 0; i < centroids.size(); i++) {
            if (cluster.get(i).size() == 0) {
                System.out.println("Cluster_center_" + i + " is empty");
                continue;
            }
            VectorConstant vector = (VectorConstant) centroids.get(i).i_emb;
            centroidFile.insert();
            centroidFile.setVal(SCHEMA_CID, new IntegerConstant(i));
            centroidFile.setVal(SCHEMA_VECTOR, vector);
        }
        centroidFile.close();
        // cluster file
        // skip for level1
        if(filenamePosfix == "") return;
        for (int i = 0; i < cluster.size(); i++) {
            if (cluster.get(i).size() == 0) {
                System.out.println("Cluster" + filenamePosfix + "_" + i + " is empty");
                continue;
            }
            String tblname = clusterTblnamePrefix + filenamePosfix + "_" + i;
            // System.out.println("Setting table for " + tblname + " with size: " + cluster.get(i).size());
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
            cluster_rf.close();
        }
    }
    public void setClusterTable(List<DataRecord> centroidLevel1, List<List<DataRecord>> clusterLevel1,
        // level1
        List<List<DataRecord>> centroidLevel2, List<List<List<DataRecord>>> clusterLevel2) {
        setClusterTable(centroidLevel1, clusterLevel1, "");
        // level2
        for(int i = 0 ; i < centroidLevel1.size() ; i++) {
            setClusterTable(centroidLevel2.get(i), clusterLevel2.get(i), "_"+i);
        }
        tableSet = true;
    }

    @Override
    public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        if (!tableSet){
            DataRecord d = new DataRecord(key.get(0), 
                new BigIntConstant(dataRecordId.block().number()), 
                new IntegerConstant(dataRecordId.id()));
            IVFIndex.data.add(d);
            return;
        }
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
