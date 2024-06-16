package org.vanilladb.core.storage.index.ivf;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.xml.crypto.Data;

import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.VectorConstant;

public class KMeans {

    private final int k;
    private final int maxIterations;
    private final DistanceFn distFn_vec;
    public List<DataRecord> centroids;

    public KMeans(int k, int maxIterations, DistanceFn distFn_vec) {
        this.k = k;
        this.maxIterations = maxIterations;
        this.distFn_vec = distFn_vec;
    }

    public List<List<DataRecord>> fit(List<DataRecord> data) {

        centroids = initializeCentroids(data);
        List<List<DataRecord>> clusters = null;

        for (int iteration = 0; iteration < maxIterations; iteration++) {
            // System.out.println("Iteration: " + iteration);
            clusters = assignClusters(data, centroids);
            List<VectorConstant> newCentroids = computeNewCentroids(clusters);

            // if (converged(centroids, newCentroids)) {
            // break;
            // }

            for (int i = 0; i < centroids.size(); i++) {
                centroids.get(i).i_emb = newCentroids.get(i);
            }
        }

        return clusters;
    }

    private List<DataRecord> initializeCentroids(List<DataRecord> data) {
        List<DataRecord> centroids = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < k; i++) {
            centroids.add(data.get(random.nextInt(data.size())));
            // System.out.println("Choose centroid: " +
            // data.get(random.nextInt(data.size())));
        }
        return centroids;
    }

    private List<List<DataRecord>> assignClusters(List<DataRecord> data, List<DataRecord> centroids) {
        List<List<DataRecord>> clusters = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            clusters.add(new ArrayList<>());
        }

        for (DataRecord record : data) {
            VectorConstant embedding = (VectorConstant) record.i_emb;
            int closestCentroidIndex = getClosestCentroid(embedding, centroids);
            // System.out.println("Record " + record.i_id + " is closest to " +
            // closestCentroidIndex);
            clusters.get(closestCentroidIndex).add(record);
        }

        return clusters;
    }

    private int getClosestCentroid(VectorConstant point, List<DataRecord> centroids) {
        double minDistance = Double.MAX_VALUE;
        int closestCentroidIndex = -1;

        for (int i = 0; i < centroids.size(); i++) {
            distFn_vec.setQueryVector((VectorConstant) centroids.get(i).i_emb);
            double distance = distFn_vec.distance(point);
            // System.out.println("Vec_a: " + centroids.get(i));
            // System.out.println("Vec_b: " + point);
            // System.out.println("Distance: " + distance);
            // System.out.println("Min Distance: " + minDistance);
            if (distance < minDistance) {
                minDistance = distance;
                closestCentroidIndex = i;
            }
        }
        // System.out.println("Minimum distance: " + minDistance);

        return closestCentroidIndex;
    }

    private List<VectorConstant> computeNewCentroids(List<List<DataRecord>> clusters) {
        List<VectorConstant> newCentroids = new ArrayList<>();
        // System.out.println("New Centroids: ");
        for (List<DataRecord> cluster : clusters) {
            newCentroids.add(computeCentroid(cluster));
            // System.out.println(computeCentroid(cluster));
        }
        return newCentroids;
    }

    private VectorConstant computeCentroid(List<DataRecord> cluster) {
        float[] centroid = new float[128];
        for (DataRecord record : cluster) {
            float[] vec = ((VectorConstant) record.i_emb).asJavaVal();
            for (int i = 0; i < centroid.length; i++) {
                centroid[i] += vec[i];
            }
        }
        for (int i = 0; i < centroid.length; i++) {
            // System.out.println("Cluster Size: " + cluster.size());
            if (cluster.size() != 0) {
                centroid[i] /= cluster.size();
            }
        }
        return new VectorConstant(centroid);
    }

    private boolean converged(List<DataRecord> oldCentroids, List<VectorConstant> newCentroids) {
        double distanceSum = 0;
        boolean isConverged = true;
        for (int i = 0; i < oldCentroids.size(); i++) {
            distFn_vec.setQueryVector((VectorConstant) oldCentroids.get(i).i_emb);
            double distance = distFn_vec.distance(newCentroids.get(i));
            distanceSum += distance;
            if (distance > 1e-4) {
                isConverged = false;
            }
        }
        System.out.println(" new centroids distance sum: " + distanceSum);
        return isConverged;
    }
}
