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
        private Double[] record;
        private int cluster_id;

        public Record(Double[] record) {
            this.record = record;
            this.cluster_id = -1;
        }

        public void setCluster(int cluster_id) {
            this.cluster_id = cluster_id;
        }

        public int getCluster() {
            return this.cluster_id;
        }

        public Double[] getRecord() {
            return this.record;
        }
    }

    private int num_items;
    private int dim;
    private int k;
    private float[][] k_centroids;
    // dynamic 2d array
    private ArrayList<Record> records = new ArrayList<Record>();
    private Random random = new Random();
    private ArrayList<Double[]> centroids;

    public kmeans(float[][] input, int num_items, int dim, int k) {
        this.num_items = num_items;
        this.dim = dim;
        this.k = k;
        this.k_centroids = new float[k][dim];
        for (int i = 0; i < num_items; i++) {
            Double[] record = new Double[dim];
            for (int j = 0; j < dim; j++) {
                record[j] = (double) input[i][j];
            }
            Record tmp_rec = new Record(record);
            records.add(tmp_rec);
        }
        centroids = kmeans_init();
    }

    private static Double recordDis(Double[] record1, Double[] record2) {
        if (record1.length != record2.length) {
            System.out.println("record length not equal");
            return Double.POSITIVE_INFINITY;
        }
        double dis = 0.0;
        for (int i = 0; i < record1.length; i++) {
            dis += Math.pow(record1[i] - record2[i], 2);
        }

        return Math.sqrt(dis);
    }

    public ArrayList<Double[]> kmeans_init() {
        System.out.println("kmeans init");
        ArrayList<Double[]> centroids = new ArrayList<Double[]>();
        // implement kmeans++ algorithm

        int first_idx = random.nextInt(num_items);
        centroids.add(records.get(first_idx).getRecord());

        for (int i = 1; i < k; i++) {
            // System.out.println("kmeans init " + i);
            double[] dist = new double[num_items];
            double sum = 0;
            for (int j = 0; j < num_items; j++) {
                double min_dist = Double.MAX_VALUE;
                for (int c = 0; c < i; c++) {
                    double d = 0;
                    d = recordDis(records.get(j).getRecord(), centroids.get(c));
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
                    centroids.add(records.get(j).getRecord());
                    records.get(j).setCluster(i);
                    break;
                }
            }
        }

        // print centroids number

        System.out.println("centroids size " + centroids.size());

        // random select k centroids, make sure no duplicate

        // ArrayList<Integer> idx = new ArrayList<Integer>();
        // for (int i = 0; i < num_items; i++) {
        // idx.add(i);
        // }

        // Collections.shuffle(idx);

        // for (int i = 0; i < k; i++) {
        // centroids.add(records.get(idx.get(i)).getRecord());
        // records.get(idx.get(i)).setCluster(i);
        // }

        return centroids;
    }

    public void run() {
        System.out.println("kmeans run");
        Double err = Double.MAX_VALUE;

        for (int i = 0; i < num_items; i++) {
            double min_dis = Double.MAX_VALUE;
            int cluster_id = records.get(i).getCluster();
            if (cluster_id != -1) {
                continue;
            }
            for (int j = 0; j < k; j++) {
                double dis = recordDis(records.get(i).getRecord(), centroids.get(j));
                if (dis < min_dis) {
                    min_dis = dis;
                    cluster_id = j;
                }
            }
            records.get(i).setCluster(cluster_id);
        }

        Boolean flag = true;
        int it = 0;
        while (it < 10 && flag) {
            // System.out.println("kmeans iteration " + it);
            it++;
            Double new_err = 0.0;
            if (flag) {
                for (int i = 0; i < k; i++) {
                    int cluster_size = 0;
                    for (int j = 0; j < dim; j++) {
                        centroids.get(i)[j] = 0.0;
                    }
                    for (int j = 0; j < num_items; j++) {
                        if (records.get(j).getCluster() == i) {
                            cluster_size++;
                            for (int l = 0; l < dim; l++) {
                                centroids.get(i)[l] += records.get(j).getRecord()[l];
                            }
                        }
                    }
                    if (cluster_size == 0) {
                        continue;
                    }
                    for (int j = 0; j < dim; j++) {
                        centroids.get(i)[j] /= (double) cluster_size;
                    }
                }
            }
            flag = false;

            for (int i = 0; i < num_items; i++) {
                int cluster_id = records.get(i).getCluster();
                Double min_dis = recordDis(records.get(i).getRecord(), centroids.get(cluster_id));
                for (int j = 0; j < k; j++) {
                    Double dis = recordDis(records.get(i).getRecord(), centroids.get(j));
                    if (dis < min_dis) {
                        min_dis = dis;
                        cluster_id = j;
                    }
                }
                if (cluster_id == -1) {
                    System.out.println("cluster id is -1");
                }
                new_err += min_dis;
                if (cluster_id != records.get(i).getCluster()) {
                    flag = true;
                    records.get(i).setCluster(cluster_id);
                }
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
                k_centroids[i][j] = centroids.get(i)[j].floatValue();
            }
        }

    }

    // return records with cluster id

    public float[][] getOutput() {

        float[][] output = new float[num_items][dim];
        for (int i = 0; i < num_items; i++) {
            for (int j = 0; j < dim; j++) {
                output[i][j] = records.get(i).getRecord()[j].floatValue();
            }
        }
        return output;
    }

    public int[] getCluster() {
        int[] cluster = new int[num_items];
        for (int i = 0; i < num_items; i++) {
            cluster[i] = records.get(i).getCluster();
        }
        return cluster;
    }

    public float[][] getCentroids() {
        return k_centroids;
    }

}
