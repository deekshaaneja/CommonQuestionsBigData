import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import scala.collection.immutable.Range;

public class findRangeUDF implements UDF2<Integer,Integer, Range> {
public static final Range serialVersionUID = new Range(0, 100, 1);
@Override
public Range call(Integer start, Integer end) throws Exception {
        return new Range(start, end, 1);
        }
        }