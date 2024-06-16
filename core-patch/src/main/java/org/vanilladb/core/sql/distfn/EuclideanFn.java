package org.vanilladb.core.sql.distfn;

import java.util.Vector;

import org.vanilladb.core.sql.VectorConstant;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorSpecies;
import jdk.incubator.vector.VectorOperators;

public class EuclideanFn extends DistanceFn {

    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
        // double sum = 0;
        // for (int i = 0; i < vec.dimension(); i++) {
        // double diff = query.get(i) - vec.get(i);
        // sum += diff * diff;
        // }

        // SIMD
        VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
        int i = 0;
        double sum = 0;
        float[] queryArr = query.asJavaVal();
        float[] vecArr = vec.asJavaVal();

        for (; i <= vec.dimension() - SPECIES.length(); i += SPECIES.length()) {
            FloatVector queryVec = FloatVector.fromArray(SPECIES, queryArr, i);
            FloatVector vecVec = FloatVector.fromArray(SPECIES, vecArr, i);
            FloatVector diff = queryVec.sub(vecVec);
            FloatVector diffSq = diff.mul(diff);
            sum += diffSq.reduceLanes(VectorOperators.ADD);
        }

        for (; i < vec.dimension(); i++) {
            float diff = queryArr[i] - vecArr[i];
            sum += diff * diff;
        }

        return Math.sqrt(sum);
    }

}
