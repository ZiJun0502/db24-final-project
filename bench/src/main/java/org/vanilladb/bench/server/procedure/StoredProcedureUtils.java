/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.bench.server.procedure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static org.vanilladb.core.sql.RecordComparator.DIR_DESC;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.RecordComparator;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.index.ivf.IVFIndex;
import org.vanilladb.core.storage.index.ivf.KMeans;
import org.vanilladb.core.storage.index.ivf.DataRecord;
import org.vanilladb.core.storage.metadata.index.IndexInfo;

public class StoredProcedureUtils {
	public static List<VectorConstant> extractVectors(List<DataRecord> records) {
		List<VectorConstant> vectors = new ArrayList<>();
		for (DataRecord record : records) {
			vectors.add((VectorConstant) record.i_emb);
		}
		return vectors;
	}

	public static void executeTrainIndex(String tableName, List<String> idxFields, String idxName, Transaction tx,
			int num_items, int dim) {
		System.out.println("Train index for: " + tableName);
		DistanceFn distFn = new EuclideanFn("i_emb");
		List<DataRecord> data = IVFIndex.data;
		System.out.println("Data Size: " + data.size());

		// level1 kmeans
		KMeans km = new KMeans(10, 20, distFn);
		System.out.println("Start training kmeans, level: 1");
		List<List<DataRecord>> clusterLevel1 = km.fit(data);
		List<DataRecord> centroidLevel1 = km.centroids;
		// level2 kmeans
		km = new KMeans(IVFIndex.getNumClusters(), 20, distFn);
		System.out.println("Start training kmeans, level: 2");
		// 10 adjacency list
		List<List<List<DataRecord>>> clusterLevel2 = new ArrayList<>();
		List<List<DataRecord>> centroidLevel2 = new ArrayList<>();
		for(int i = 0 ; i < 10 ; i++) {
			List<List<DataRecord>> cluster = km.fit(clusterLevel1.get(i));
			List<DataRecord> centroid = km.centroids;

			clusterLevel2.add(cluster);
			centroidLevel2.add(centroid);
		}
		System.out.println("Setting index cluster table");

		List<IndexInfo> iis = VanillaDb.catalogMgr().getIndexInfo(tableName, idxFields.get(0), tx);
		IVFIndex ivf = (IVFIndex) iis.get(0).open(tx);
		System.out.println("Level1 Centroid Size: " + centroidLevel1.size());
		for(int i = 0 ; i < 10 ; i++) {
			System.out.println("Level1 cluster Size: " + clusterLevel1.get(i).size());
			System.out.println("	Level2 Centroid Size: " + centroidLevel2.get(i).size());
		}
		ivf.setClusterTable(centroidLevel1, clusterLevel1, 
			centroidLevel2, clusterLevel2);
		ivf.preLoadToMemory();
		IVFIndex.data.clear();
	}

	public static Scan executeQuery(String sql, Transaction tx) {
		Plan p = VanillaDb.newPlanner().createQueryPlan(sql, tx);
		return p.open();
	}

	public static int executeUpdate(String sql, Transaction tx) {
		return VanillaDb.newPlanner().executeUpdate(sql, tx);
	}

	static class MapRecord implements Record {

		Map<String, Constant> fldVals = new HashMap<>();

		@Override
		public Constant getVal(String fldName) {
			return fldVals.get(fldName);
		}

		public void put(String fldName, Constant val) {
			fldVals.put(fldName, val);
		}

		public boolean containsKey(String fldName) {
			return fldVals.containsKey(fldName);
		}
	}

	static class PriorityQueueScan implements Scan {
		private PriorityQueue<MapRecord> pq;
		private boolean isBeforeFirsted = false;

		public PriorityQueueScan(PriorityQueue<MapRecord> pq) {
			this.pq = pq;
		}

		@Override
		public Constant getVal(String fldName) {
			return pq.peek().getVal(fldName);
		}

		@Override
		public void beforeFirst() {
			this.isBeforeFirsted = true;
		}

		@Override
		public boolean next() {
			if (isBeforeFirsted) {
				isBeforeFirsted = false;
				return true;
			}
			pq.poll();
			return pq.size() > 0;
		}

		@Override
		public void close() {
			return;
		}

		@Override
		public boolean hasField(String fldName) {
			return pq.peek().containsKey(fldName);
		}
	}

	public static Scan executeCalculateRecall(VectorConstant query, String tableName, String field, int limit,
			Transaction tx) {
		Plan p = new TablePlan(tableName, tx);

		DistanceFn distFn = new EuclideanFn(field);
		distFn.setQueryVector(query);

		List<String> sortFlds = new ArrayList<String>();
		sortFlds.add(distFn.fieldName());

		List<Integer> sortDirs = new ArrayList<Integer>();
		sortDirs.add(DIR_DESC); // for priority queue

		RecordComparator comp = new RecordComparator(sortFlds, sortDirs, distFn);

		PriorityQueue<MapRecord> pq = new PriorityQueue<>(limit, (MapRecord r1, MapRecord r2) -> comp.compare(r1, r2));

		Scan s = p.open();
		s.beforeFirst();
		while (s.next()) {
			MapRecord fldVals = new MapRecord();
			for (String fldName : p.schema().fields()) {
				fldVals.put(fldName, s.getVal(fldName));
			}
			pq.add(fldVals);
			if (pq.size() > limit)
				pq.poll();
		}
		s.close();

		s = new PriorityQueueScan(pq);

		return s;
	}

	public static int executeInsert(InsertData sql, Transaction tx) {
		return VanillaDb.newPlanner().executeInsert(sql, tx);
	}
}
