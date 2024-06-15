package org.vanilladb.core.storage.index.ivf;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class KMeans {

    private final int k;
    private final int maxIterations;
    private final DistanceFn distFn_vec;

    public KMeans(int k, int maxIterations, DistanceFn distFn_vec) {
        this.k = k;
        this.maxIterations = maxIterations;
        this.distFn_vec = distFn_vec;
    }

    public List<List<VectorConstant>> fit(List<VectorConstant> data) {
        List<VectorConstant> centroids = initializeCentroids(data);
        List<List<VectorConstant>> clusters = null;

        for (int iteration = 0; iteration < maxIterations; iteration++) {
            clusters = assignClusters(data, centroids);
            List<VectorConstant> newCentroids = computeNewCentroids(clusters);

            if (converged(centroids, newCentroids)) {
                break;
            }

            centroids = newCentroids;
        }

        return clusters;
    }

    private List<VectorConstant> initializeCentroids(List<VectorConstant> data) {
        List<VectorConstant> centroids = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < k; i++) {
            centroids.add(data.get(random.nextInt(data.size())));
        }
        return centroids;
    }

    private List<List<VectorConstant>> assignClusters(List<VectorConstant> data, List<VectorConstant> centroids) {
        List<List<VectorConstant>> clusters = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            clusters.add(new ArrayList<>());
        }

        for (VectorConstant point : data) {
            int closestCentroidIndex = getClosestCentroid(point, centroids);
            clusters.get(closestCentroidIndex).add(point);
        }

        return clusters;
    }

    private int getClosestCentroid(VectorConstant point, List<VectorConstant> centroids) {
        double minDistance = Double.MAX_VALUE;
        int closestCentroidIndex = -1;

        for (int i = 0; i < centroids.size(); i++) {
            distFn_vec.setQueryVector(centroids.get(i));
            double distance = distFn_vec.distance(point);
            if (distance < minDistance) {
                minDistance = distance;
                closestCentroidIndex = i;
            }
        }

        return closestCentroidIndex;
    }

    private List<VectorConstant> computeNewCentroids(List<List<VectorConstant>> clusters) {
        List<VectorConstant> newCentroids = new ArrayList<>();
        for (List<VectorConstant> cluster : clusters) {
            newCentroids.add(computeCentroid(cluster));
        }
        return newCentroids;
    }

    private VectorConstant computeCentroid(List<VectorConstant> cluster) {
        float[] centroid = new float[128];
        for (VectorConstant point : cluster) {
            float[] vec = point.asJavaVal();
            for (int i = 0; i < centroid.length; i++) {
                centroid[i] += vec[i];
            }
        }
        for (int i = 0; i < centroid.length; i++) {
            centroid[i] /= cluster.size();
        }
        return new VectorConstant(centroid);
    }

    private boolean converged(List<VectorConstant> oldCentroids, List<VectorConstant> newCentroids) {
        for (int i = 0; i < oldCentroids.size(); i++) {
            distFn_vec.setQueryVector(oldCentroids.get(i));
            if (distFn_vec.distance(newCentroids.get(i)) > 1e-4) {
                return false;
            }
        }
        return true;
    }

    public static class VectorConstant {
        private final float[] vec;

        public VectorConstant(float[] vec) {
            this.vec = vec;
        }

        public float[] asJavaVal() {
            return vec;
        }
    }

    public interface DistanceFn {
        void setQueryVector(VectorConstant v0);
        double distance(VectorConstant v1);
    }
}
