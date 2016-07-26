package datacube;
/* Java imports */
import java.util.List;
/* Spark imports */
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
/* Local imports */
import datacube.data.DCObject;
import datacube.data.DCDimension;
import datacube.data.Property;
import datacube.data.DoubleProperty;
import datacube.data.PropertyName;

/**
 * Calculate appropriate datacube bucket-id for each object
 * @return 'DataCube Bucket-ID', 'Object-ID'
 */
public class DCObjectMap implements PairFunction<DCObject, Integer, String>{

private final List<DCDimension> dimensions;

public DCObjectMap(List<DCDimension> dimensions){
        this.dimensions = dimensions;
}

private int getMultiplier(int index){
        int multiplier = 1;
        for (int i=dimensions.size()-1; i>index; --i) {
                multiplier *= dimensions.get(i).getBucketCount();
        }
        return multiplier;
}

//  /**
//   * Area 0-100, resolution: 10, buckets = 10 (i=0: 0<=Area<10, i=1: 10<=Area<20 ...)
//    * ELongation 0-2, resolution: 0.1, buckets = 20 (j=0: 0<=Elongation<0.1, j=1: 0.1<=ELongation<0.2 ...)
//     */
private int mapIndex(int...indices){
        if (indices.length != dimensions.size())
                throw new RuntimeException("[DataCube] Invalid indices");
        // change to more than int
        int linearIndex = 0;
        int i;
        for (i=0; i<(indices.length-1); ++i) {
                if (indices[i] > (dimensions.get(i).getBucketCount()-1)) {
                        String str = "[DataCube] Index out of bound, index: "+
                                     indices[i]+", max: "+(dimensions.get(i).getBucketCount()-1);
                        throw new RuntimeException(str);
                }

                linearIndex += indices[i] * getMultiplier(i);
        }
        if (indices[i] > (dimensions.get(i).getBucketCount()-1)) {
                String str = "[DataCube] Index out of bound, index: "+indices[i]+", max: "+(dimensions.get(i).getBucketCount()-1);
                throw new RuntimeException(str);
        }

        linearIndex += indices[i];
        return linearIndex;
}

private Double getValue(PropertyName prop, DCObject obj){
        for (Property p : obj.props) {
                if ((p instanceof DoubleProperty) && (p.getNameStr().equals(prop.value))) {
                        return ((DoubleProperty)p).getValue();
                }
        }
        return null;
}

private int getIndex(Double value, DCDimension dim){
        // do it using max ...
        int index = 0;
        final Double resolution = dim.getResolution();
        // (resolution < 1) -> larger index value
        //if (resolution >= 1){
        int ret = (int)(value/resolution);
        // corner case: (value = max) -> (ret = bucketCount)
        return (ret >= dim.getBucketCount()) ? dim.getBucketCount()-1 : ret;
        //}
        // else{
        //  for (double i=dim.getMin(); i<dim.getMax() ; i+=dim.getResolution()){
        //      if ((value >= i) && (value < (i+dim.getResolution())) )
        //    return index;
        //      index++;
        //  }
        // }
        // return (index-1);
}

public Tuple2<Integer, String> call(DCObject obj){

        String ret = obj.getID();

        int[] indices = new int[dimensions.size()];
        int i=0;
        for (DCDimension dim : dimensions) {
                Double curr_value = getValue(dim.getName(), obj);
                int index = getIndex(curr_value, dim);

                if (index > (dim.getBucketCount()-1) || (index < 0)) {
                        throw new RuntimeException("[DataCube] Index out of bound"+
                                                   ", index: "+ index +
                                                   ", max: "+ (dimensions.get(i).getBucketCount()-1)+
                                                   ", value: " + curr_value +
                                                   ", name: " + dim.getNameStr()
                                                   );
                }
                indices[i++] = index;

                if (DataCube.DEBUG) {
                        // ret += "\t" + dim.getName().value + ":" + curr_value + "\tMin:"+dim.getMin() + "\tMax:" + dim.getMax() + "\tResolution:" + dim.getResolution() + "\t";
                        ret += "\t" + dim.getName().value + ":" + curr_value + "\t";
                }
        }

        if (DataCube.DEBUG) {
                ret += "(";
                for (int ind : indices)
                        ret += ind + ",";
                ret += ")";
        }

        // map indices
        int lIndex = mapIndex(indices);
        // System.out.println(ret);
        System.out.println("buiilding index");
        return new Tuple2<Integer, String>(lIndex, ret);
}
}
