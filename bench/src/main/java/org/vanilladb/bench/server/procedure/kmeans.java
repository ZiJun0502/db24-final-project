package org.vanilladb.bench.server.procedure;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Random;
import java.util.List;
import java.lang.Math;
import java.util.Collections;
import java.io.*;
// import vector constant
import org.vanilladb.core.sql.VectorConstant;

public class kmeans {

    public static class Record {
        Double[] record;
        int cluster_id;

        public Record(Double[] record) {
            this.record = record;
        }

        public void setCluster(int cluster_id) {
            this.cluster_id = cluster_id;
        }

        public int getCluster() {
            return cluster_id;
        }

        public Double[] getRecord() {
            return record;
        }
    }

    private int num_items;
    private int dim;
    private int k;
    private int[][] k_centroids;
    // dynamic 2d array
    private List<List<Record>> clusters;
    private Record[] records;
    private Random random = new Random();
    private ArrayList<Double[]> centroids;

    public kmeans(int[][] input, int num_items, int dim, int k) {
        this.num_items = num_items;
        this.dim = dim;
        this.k = k;
        this.k_centroids = new int[k][dim];
        this.records = new Record[num_items];
        this.clusters = new ArrayList<List<Record>>(k);
        for (int i = 0; i < num_items; i++) {
            Double[] record = new Double[dim];
            for (int j = 0; j < dim; j++) {
                record[j] = (double) input[i][j];
            }
            records[i] = new Record(record);
        }
        centroids = kmeans_init();
    }

    private static Double recordDis(Double[] record1, Double[] record2) {
        if (record1.length != record2.length) {
            System.out.println("record length not equal");
            return Double.POSITIVE_INFINITY;
        }
        double dis = 0;
        for (int i = 0; i < record1.length; i++) {
            dis += Math.pow(record1[i] - record2[i], 2);
        }

        return Math.sqrt(dis);
    }

    public ArrayList<Double[]> kmeans_init() {
        ArrayList<Double[]> centroids = new ArrayList<Double[]>();
        // implement kmeans++ algorithm

        int first_idx = random.nextInt(num_items);
        centroids.add(records[first_idx].getRecord());

        for (int i = 1; i < k; i++) {
            double[] dist = new double[num_items];
            double sum = 0;
            for (int j = 0; j < num_items; j++) {
                double min_dist = Double.MAX_VALUE;
                for (int c = 0; c < i; c++) {
                    double d = 0;
                    d = recordDis(records[j].getRecord(), centroids.get(c));
                    min_dist = Math.min(min_dist, d);
                }
                dist[j] = min_dist;
                sum += min_dist;
            }

            double r = random.nextDouble() * sum;
            double tmp = 0;
            for (int j = 0; j < num_items; j++) {
                tmp += dist[j];
                if (tmp >= r) {
                    centroids.add(records[j].getRecord());
                    break;
                }
            }
        }

        return centroids;
    }

    public void run() {

        Double err = Double.MAX_VALUE;
        // init cluster

        for (int i = 0; i < num_items; i++) {
            double min_dis = Double.MAX_VALUE;
            int cluster_id = -1;
            for (int j = 0; j < k; j++) {
                double dis = recordDis(records[i].getRecord(), centroids.get(j));
                if (dis < min_dis) {
                    min_dis = dis;
                    cluster_id = j;
                }
            }
            records[i].setCluster(cluster_id);
            clusters.get(cluster_id).add(records[i]);
        }

        int it = 0;
        while (it < 100) {
            it++;
            Double new_err = 0.0;
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < dim; j++) {
                    centroids.get(i)[j] = 0.0;
                }
                for (int j = 0; j < clusters.get(i).size(); j++) {
                    for (int l = 0; l < dim; l++) {
                        centroids.get(i)[l] += clusters.get(i).get(j).getRecord()[l];
                    }
                }
                for (int j = 0; j < dim; j++) {
                    centroids.get(i)[j] /= clusters.get(i).size();
                }
            }

            for (int i = 0; i < num_items; i++) {
                double min_dis = Double.MAX_VALUE;
                int cluster_id = -1;
                for (int j = 0; j < k; j++) {
                    double dis = recordDis(records[i].getRecord(), centroids.get(j));
                    if (dis < min_dis) {
                        min_dis = dis;
                        cluster_id = j;
                    }
                }
                new_err += min_dis;
                records[i].setCluster(cluster_id);
                clusters.get(cluster_id).add(records[i]);
            }

            if (Math.abs(err - new_err) < 1e-6) {
                System.out.println("kmeans converge" + Math.abs(err - new_err));
                break;
            }
            err = new_err;
        }

        // store centroids to k_centroids
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < dim; j++) {
                k_centroids[i][j] = centroids.get(i)[j].intValue();
            }
        }

    }

    // return records with cluster id

    public int[][] getOutput() {

        int[][] output = new int[num_items][dim + 1];
        for (int i = 0; i < num_items; i++) {
            for (int j = 0; j < dim; j++) {
                output[i][j] = records[i].getRecord()[j].intValue();
            }
            output[i][dim] = records[i].getCluster();
        }
        return output;
    }

    public int[][] getCentroids() {
        return k_centroids;
    }

}
