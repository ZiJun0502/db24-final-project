package org.vanilladb.core.storage.index.ivf;

import static org.vanilladb.core.sql.Type.DOUBLE;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VECTOR;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Vector;
import java.util.Arrays;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.record.RecordPage;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.util.CoreProperties;

public class IVFIndex extends Index {

    private static final String 
        SCHEMA_ID = "i_id", SCHEMA_CID = "c_id", SCHEMA_VECTOR = "i_emb",
        SCHEMA_RID_BLOCK = "block", SCHEMA_RID_ID = "id";
    private static final int NUM_CLUSTERS;

    static {
        NUM_CLUSTERS = CoreProperties.getLoader().getPropertyAsInteger(IVFIndex.class.getName() + ".NUM_CLUSTERS", 100);
    }
	// private static String vecFieldName(int index) {
	// 	return SCHEMA_VECTOR_PREFIX + index;
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
	 *            the type of the indexed field
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
    private int numClusters;
    private int clusterID;
	private SearchKey searchKey;
	private RecordFile rf;
	private boolean isBeforeFirsted;
    private String centroidTblname, clusterTblnamePrefix;
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
        // System.out.println("IVFIndex extractVector: " + Arrays.toString(vec.asJavaVal()));
        return vec;
    }
    private int searchClosestCluster(VectorConstant vec) {
        System.out.println("searchClosestCluster: " + centroidTblname);
        double minDistance = Double.MAX_VALUE;
        this.distFn_vec.setQueryVector(vec);
        int closestClusterIndex = -1;
        TableInfo ti = new TableInfo(centroidTblname, schema_centroid(keyType));
        // open centroid file
		this.rf = ti.open(tx, false);
        rf.beforeFirst();
        int num_centroids = 0;
        while(rf.next()) {
            num_centroids++;
            Constant cid = rf.getVal(SCHEMA_CID);
            Constant centroid_vec = rf.getVal(SCHEMA_VECTOR);
            // System.out.println("cid: " + cid + " block: " + block + 
                // " id: " + id + "vec: " + centroid_vec);
            System.out.println("cid: " + cid + "vec: " + centroid_vec);
            double distance = distFn_vec.distance((VectorConstant)centroid_vec);
            if (distance < minDistance) {
                minDistance = distance;
                closestClusterIndex = (int) cid.asJavaVal();
            }
        }

        System.out.println("num_centroids in file" + num_centroids);
        System.out.println("closestCluster: " + closestClusterIndex);
        return closestClusterIndex;
    }
    @Override
    public void beforeFirst(SearchRange searchRange) {
        close();
        // support the range query only for this IVF index implementation
        if (!searchRange.isSingleValue())
            throw new UnsupportedOperationException();

        this.searchKey = searchRange.asSearchKey();
        VectorConstant queryVec= extractVector(searchKey);
        this.clusterID = searchClosestCluster(queryVec);

        String tblname = this.clusterTblnamePrefix + this.clusterID;
        TableInfo ti = new TableInfo(tblname, schema_cluster(keyType));
        System.out.println("Opening cluster file: " + tblname + ".tbl");
		// the underlying record file should not perform logging
		this.rf = ti.open(tx, false);

		// initialize the file header if needed
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
		return false;
    }

    @Override
    public RecordId getDataRecordId() {
        long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
        int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
        return new RecordId(new BlockId(dataFileName, blkNum), id);
    }

    @Override
    public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
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
	private SearchKey getKey() {
		Constant[] vals = new Constant[keyType.length()];
		for (int i = 0; i < vals.length; i++)
			vals[i] = rf.getVal(SCHEMA_VECTOR);
		return new SearchKey(vals);
	}
}
