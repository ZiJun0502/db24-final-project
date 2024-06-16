package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class EuclideanFn extends DistanceFn {
    private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
        double sum = 0;
        float[] query = this.query.asJavaVal();
        float[] vector = vec.asJavaVal();
        for (int i = 0; i < query.length; i += SPECIES.length()) {
            VectorMask<Float> m = SPECIES.indexInRange(i, query.length);
            FloatVector a = FloatVector.fromArray(SPECIES, query, i, m);
            FloatVector b = FloatVector.fromArray(SPECIES, vector, i, m);
            FloatVector diff = a.sub(b);
            diff = diff.mul(diff);
            sum += diff.reduceLanes(VectorOperators.ADD);
        }

        return Math.sqrt(sum);
    }

}