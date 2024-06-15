package org.vanilladb.bench.server.procedure.sift;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;

import org.vanilladb.bench.benchmarks.sift.SiftBenchConstants;
import org.vanilladb.bench.server.param.sift.SiftTestbedLoaderParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureUtils;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

// import kmeans class
import org.vanilladb.bench.server.procedure.kmeans;

public class SiftTestbedLoaderProc extends StoredProcedure<SiftTestbedLoaderParamHelper> {
    private static Logger logger = Logger.getLogger(SiftTestbedLoaderProc.class.getName());

    public SiftTestbedLoaderProc() {
        super(new SiftTestbedLoaderParamHelper());
    }

    @Override
    protected void executeSql() {
        if (logger.isLoggable(Level.INFO))
            logger.info("Start loading testbed...");

        // turn off logging set value to speed up loading process
        RecoveryMgr.enableLogging(false);

        dropOldData();
        createSchemas();

        // Generate item records
        generateItems(0);

        // if (logger.isLoggable(Level.INFO))
        //     logger.info("Training IVF index...");

        // StoredProcedureUtils.executeTrainIndex(getHelper().getTableName(),
        // getHelper().getIdxFields(),
        // getHelper().getIdxName(), getTransaction());

        if (logger.isLoggable(Level.INFO))
            logger.info("Loading completed. Flush all loading data to disks...");

        RecoveryMgr.enableLogging(true);

        // Create a checkpoint
        CheckpointTask cpt = new CheckpointTask();
        cpt.createCheckpoint();

        // Delete the log file and create a new one
        VanillaDb.logMgr().removeAndCreateNewLog();

        if (logger.isLoggable(Level.INFO))
            logger.info("Loading procedure finished.!!!");
    }

    private void dropOldData() {
        if (logger.isLoggable(Level.WARNING))
            logger.warning("Dropping is skipped.");
    }

    private void createSchemas() {
        SiftTestbedLoaderParamHelper paramHelper = getHelper();
        Transaction tx = getTransaction();

        if (logger.isLoggable(Level.FINE))
            logger.info("Creating tables...");

        for (String sql : paramHelper.getTableSchemas())
            StoredProcedureUtils.executeUpdate(sql, tx);

        if (logger.isLoggable(Level.INFO))
            logger.info("Creating indexes...");

        // Create indexes
        for (String sql : paramHelper.getIndexSchemas()) {
            // System.out.println(sql);
            StoredProcedureUtils.executeUpdate(sql, tx);
        }

        // shuchen
        // Create cluster tables
        for (String sql : paramHelper.getClusterSchemas()) {
            StoredProcedureUtils.executeUpdate(sql, tx);
        }
        // Create cluster center table
        for (String sql : paramHelper.getClusterCenterSchemas()) {
            StoredProcedureUtils.executeUpdate(sql, tx);
        }

        if (logger.isLoggable(Level.FINE))
            logger.info("Finish creating schemas.");
    }

    private void generateItems(int startIId) {
        if (logger.isLoggable(Level.FINE))
            logger.info("Start populating items from SIFT1M dataset");

        Transaction tx = getTransaction();

        int dim = SiftBenchConstants.NUM_DIMENSION;
        int num_items = SiftBenchConstants.NUM_ITEMS;

        float[][] kmeans_input = new float[num_items][dim];

        try (BufferedReader br = new BufferedReader(new FileReader(SiftBenchConstants.DATASET_FILE))) {
            int iid = startIId;
            String vectorString;

            while (iid < SiftBenchConstants.NUM_ITEMS && (vectorString = br.readLine()) != null) {

                String[] temp_vec = vectorString.split(" ");
                for (int i = 0; i < dim; i++) {
                    kmeans_input[iid][i] = Float.parseFloat(temp_vec[i]);
                }

                String sql = "INSERT INTO sift(i_id, i_emb) VALUES (" + iid + ", [" + vectorString + "])";
                // logger.info(sql);
                iid++;
                StoredProcedureUtils.executeUpdate(sql, tx);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create kmeans object

        int k = 400;

        System.out.println("Start kmeans clustering...");

        kmeans Kmeans = new kmeans(kmeans_input, num_items, dim, k);
        Kmeans.run();

        float[][] kmeans_output = Kmeans.getOutput();
        int[] cluster_id = Kmeans.getCluster();

        int[] cluster_size = new int[k];
        for (int i = 0; i < num_items; i++) {
            cluster_size[cluster_id[i]]++;
        }

        for (int i = 0; i < k; i++) {
            if (cluster_size[i] == 0) {
                System.out.println("cluster " + i + " size is 0");
            }
        }

        for (int i = 0; i < num_items; i++) {
            int cur_cluster = cluster_id[i];
            float[] rawVector = new float[dim];
            for (int j = 0; j < dim; j++) {
                rawVector[j] = kmeans_output[i][j];
            }
            VectorConstant vector = new VectorConstant(rawVector);
            String s_vec = vector.toString();
            // for (int j = 0; j < dim; j++) {
            // System.out.print(vector.asJavaVal()[j] + " ");
            // }
            // System.out.println();
            String sql = "INSERT INTO cluster_" + cur_cluster + "(i_id, i_emb) VALUES ("
                    + i + ", "
                    + s_vec
                    + ")";
            // System.out.println(sql);
            StoredProcedureUtils.executeUpdate(sql, tx);
        }

        float[][] kmeans_centroids = Kmeans.getCentroids();
        // StoredProcedureUtils.loadToIndex(k, kmeans_centroids);
        for (int i = 0; i < k; i++) {
            float[] rawVector = new float[dim];
            for (int j = 0; j < dim; j++) {
                rawVector[j] = kmeans_centroids[i][j];
            }
            VectorConstant vector = new VectorConstant(rawVector);
            String sql = "INSERT INTO cluster_center(c_id, i_emb) VALUES (" + i + ", " + vector.toString() + ")";
            // System.out.println(sql);
            StoredProcedureUtils.executeUpdate(sql, tx);
        }

        if (logger.isLoggable(Level.FINE))
            logger.info("Finish populating items.");
    }
}
