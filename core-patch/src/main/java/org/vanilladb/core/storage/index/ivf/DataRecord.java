package org.vanilladb.core.storage.index.ivf;

import org.vanilladb.core.sql.Constant;
public class DataRecord {
    // public Constant i_id;
    public Constant i_emb;
    public Constant blockNum;
    public Constant recordId;

    public DataRecord(Constant i_emb, Constant blockNum, Constant recordId) {
        // this.i_id = i_id;
        this.i_emb = i_emb;
        this.blockNum = blockNum;
        this.recordId = recordId;
    }
}
