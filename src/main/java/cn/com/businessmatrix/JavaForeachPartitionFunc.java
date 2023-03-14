package cn.com.businessmatrix;

import org.apache.spark.sql.Row;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.Serializable;

public abstract class JavaForeachPartitionFunc extends AbstractFunction1<Iterator<Row>, BoxedUnit> implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 2682724351239860311L;

    @Override
    public BoxedUnit apply(Iterator<Row> it) {
        call(it);
        return BoxedUnit.UNIT;
    }

    public abstract void call(Iterator<Row> it);
}