package org.vanilladb.bench.server.param.sift;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureHelper;

public class SiftTestbedLoaderParamHelper implements StoredProcedureHelper {

    private static final String TABLES_DDL[] = new String[1];
    private static final String INDEXES_DDL[] = new String[1];
    private static final int N_DIM = 128;
    private static final int N_CLUSTERS = 100;
    private static final String Cluster_table[] = new String[N_CLUSTERS];
    private static final String Cluster_center[] = new String[1];

    private int numItems;

    public String getTableName() {
        return "sift";
    }

    public String getIdxName() {
        return "idx_sift";
    }

    public List<String> getIdxFields() {
        List<String> embFields = new ArrayList<String>(1);
        embFields.add("i_emb");
        return embFields;
    }

    public String[] getTableSchemas() {
        return TABLES_DDL;
    }

    public String[] getIndexSchemas() {
        return INDEXES_DDL;
    }

    // shuchen
    public String[] getClusterSchemas() {
        return Cluster_table;
    }

    public String[] getClusterCenterSchemas() {
        return Cluster_center;
    }

    public int getNumberOfItems() {
        return numItems;
    }

    @Override
    public void prepareParameters(Object... pars) {
        numItems = (Integer) pars[0];
        TABLES_DDL[0] = "CREATE TABLE " + getTableName() + " (i_id INT, i_emb VECTOR(" + N_DIM + "))";
        INDEXES_DDL[0] = "CREATE INDEX " + getIdxName() + " ON sift (" +
                getIdxFields().get(0) + ") USING IVF";
    }

    @Override
    public Schema getResultSetSchema() {
        return new Schema();
    }

    @Override
    public SpResultRecord newResultSetRecord() {
        return new SpResultRecord();
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
}
