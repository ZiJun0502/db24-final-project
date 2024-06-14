package org.vanilladb.core.storage.index.ivf;

import static org.vanilladb.core.sql.Type.DOUBLE;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

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
        SCHEMA_VECTOR_PREFIX = "vec", SCHEMA_RID_BLOCK = "block", SCHEMA_RID_ID = "id",
        SCHEMA_CLUSTER_ID = "cluster_id", SCHEMA_DATA_PAGE = "dt_page";
    private static final int NUM_CLUSTERS;

    static {
        NUM_CLUSTERS = CoreProperties.getLoader().getPropertyAsInteger(IVFIndex.class.getName() + ".NUM_CLUSTERS", 100);
    }
	private static String vecFieldName(int index) {
		return SCHEMA_VECTOR_PREFIX + index;
	}
	public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
        // copied from HashIndex.java
		int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(schema(keyType));
		return (totRecs / rpb) / NUM_CLUSTERS;
	}

	/**
	 * Returns the schema of the index records.
	 * 
	 * @param fldType
	 *            the type of the indexed field
	 * 
	 * @return the schema of the index records
	 */
	private static Schema schema(SearchKeyType keyType) {
        Schema sch = new Schema();
        for (int i = 0; i < keyType.length(); i++) {
            sch.addField(SCHEMA_VECTOR_PREFIX + i, keyType.get(i));
        }
        sch.addField(SCHEMA_RID_BLOCK, BIGINT);
        sch.addField(SCHEMA_RID_ID, INTEGER);
        return sch;
    }
    // Helper class to represent a vector record
    private static class VectorRecord {
        VectorConstant vec;
        RecordId recordId;

        VectorRecord(VectorConstant vec, RecordId recordId) {
            this.vec= vec;
            this.recordId = recordId;
        }
    }
    private int numClusters;
    private int clusterID;
    private List<List<VectorRecord>> clusters;
	private SearchKey searchKey;
	private RecordFile rf;
	private boolean isBeforeFirsted;
    DistanceFn distFn_vec;
    public IVFIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
        super(ii, keyType, tx);
        this.clusters = new ArrayList<>();
        this.distFn_vec = new EuclideanFn("i_emb");
    }
    public static void train(String tblname, List<String> embFields, 
            String idxName, Transaction tx) {
    }
    @Override
    public void preLoadToMemory() {
        for (int i = 0; i < numClusters; i++) {
            String tblname = ii.indexName() + i + ".tbl";
            long size = fileSize(tblname);
            BlockId blk;
            for (int j = 0; j < size; j++) {
                blk = new BlockId(tblname, j);
                tx.bufferMgr().pin(blk);
            }
        }
    }
    private VectorConstant extractVector(SearchKey key) {
        VectorConstant vec = (VectorConstant) key.get(0);
        // System.out.println("IVFIndex extractVector: " + Arrays.toString(vec.asJavaVal()));
        return vec;
    }
    private int searchClosestCluster(VectorConstant vec) {
        double minDistance = Double.MAX_VALUE;
        this.distFn_vec.setQueryVector(vec);
        int closestClusterIndex = -1;

        for (int clusterIndex = 0; clusterIndex < clusters.size(); clusterIndex++) {
            List<VectorRecord> cluster = clusters.get(clusterIndex);
            for (VectorRecord record : cluster) {
                double distance = distFn_vec.distance(record.vec);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestClusterIndex = clusterIndex;
                }
            }
        }
        // System.out.println("closestCluster: " + closestClusterIndex);
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

        String tblname = ii.indexName() + this.clusterID;
        TableInfo ti = new TableInfo(tblname, schema(keyType));
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
			if (getKey().equals(searchKey))
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
		for (int i = 0; i < keyType.length(); i++)
			rf.setVal(vecFieldName(i), key.get(i));
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
			vals[i] = rf.getVal(vecFieldName(i));
		return new SearchKey(vals);
	}
}
