package cn.com.businessmatrix;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JavaPairFlatMapPartitionFunc implements PairFlatMapFunction<Iterator<Row>, Object, Row>, Serializable {


    private static final long serialVersionUID = 1256709393124504459L;

    @Override
    public Iterator<Tuple2<Object, Row>> call(Iterator<Row> arg0)
            throws Exception {
        List<Tuple2<Object, Row>> list = new ArrayList<Tuple2<Object, Row>>();
        while (arg0.hasNext()) {
            Row row = arg0.next();
            list.add(new Tuple2<Object, Row>((String) row.getAs(0), row));
        }
        return list.iterator();
    }

}