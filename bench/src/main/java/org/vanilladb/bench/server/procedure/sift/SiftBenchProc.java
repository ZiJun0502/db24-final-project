package org.vanilladb.bench.server.procedure.sift;

import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.vanilladb.bench.benchmarks.sift.SiftBenchConstants;
import org.vanilladb.bench.server.param.sift.SiftBenchParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureUtils;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

class Pair implements Comparable<Pair> {
    Integer key;
    float value;

    public Pair(Integer key, float value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(Pair other) {
        return Float.compare(this.value, other.value);
    }
}

public class SiftBenchProc extends StoredProcedure<SiftBenchParamHelper> {

    private static final int top_K_cluster = 20;

    public SiftBenchProc() {
        super(new SiftBenchParamHelper());
    }

    @Override
    protected void executeSql() {
        SiftBenchParamHelper paramHelper = getHelper();
        VectorConstant query = paramHelper.getQuery();
        Transaction tx = getTransaction();

        Set<Integer> nearestNeighbors = new HashSet<>();
        float[] query_emb = query.asJavaVal();

        int dim = SiftBenchConstants.NUM_DIMENSION;
        int cluster_id[] = new int[top_K_cluster];
        String CenterQuery = "SELECT c_id, i_emb FROM cluster_center";
        Scan near_center = StoredProcedureUtils.executeQuery(CenterQuery, tx);
        HashMap<Integer, float[]> center = new HashMap<>();
        near_center.beforeFirst();
        while (near_center.next()) {
            int c_id = (Integer) near_center.getVal("c_id").asJavaVal();
            float[] c_emb = (float[]) near_center.getVal("i_emb").asJavaVal();
            center.put(c_id, c_emb);
        }

        near_center.close();

        // find the top 20 nearest cluster centroid and store in cluster_id[]

        // calculate the distance between query and each cluster centroid and sort

        List<Pair> dist = new ArrayList<>();
        for (Integer key : center.keySet()) {
            float[] c_emb = center.get(key);
            float sum = 0;
            for (int i = 0; i < dim; i++) {
                float diff = query_emb[i] - c_emb[i];
                sum += diff * diff;
            }
            dist.add(new Pair(key, sum));
        }

        Collections.sort(dist);
        int cnt_k = 0;

        for (Pair pair : dist) {
            cluster_id[cnt_k] = pair.key;
            cnt_k++;
            if (cnt_k >= top_K_cluster)
                break;
        }

        int count = 0;

        // in top k clusters, find the nearest point in each cluster
        // store the result in nearestNeighbors

        for (int i = 0; i < top_K_cluster; i++) {
            String ClusterQuery = "SELECT i_id, i_emb FROM cluster_" + cluster_id[i];
            Scan near_cluster = StoredProcedureUtils.executeQuery(ClusterQuery, tx);
            near_cluster.beforeFirst();
            float min_dis = Float.MAX_VALUE;
            int nearest_id = -1;
            while (near_cluster.next()) {
                int i_id = (Integer) near_cluster.getVal("i_id").asJavaVal();
                float[] i_emb = (float[]) near_cluster.getVal("i_emb").asJavaVal();
                float sum = 0;
                for (int j = 0; j < dim; j++) {
                    float diff = query_emb[j] - i_emb[j];
                    sum += diff * diff;
                }
                if (sum < min_dis) {
                    min_dis = sum;
                    nearest_id = i_id;
                }
            }
            if (nearest_id != -1) {
                nearestNeighbors.add(nearest_id);
                System.out.println("nearest_id: " + nearest_id);
                count++;
            }
            near_cluster.close();
        }

        if (count == 0)
            throw new RuntimeException("Nearest neighbor query execution failed for " + query.toString());

        paramHelper.setNearestNeighbors(nearestNeighbors);
    }

}
