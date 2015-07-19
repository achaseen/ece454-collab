import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import java.util.Iterator;
import org.apache.hadoop.io.WritableComparable;

public class Part2 extends EvalFunc<DataBag> {

    BagFactory bagFactory = BagFactory.getInstance();
    TupleFactory tupleFactory = TupleFactory.getInstance();
    private static final double NORMAL_EXP_VAL = 0.5;

    /* In this function, we are getting a bag of tuples of genes from each sample
     * and we return a bag of tuples of genes that are above the NORMAL_EXP_VAL
     * Input:   { (0.0), (0.222), (0.689), (0.888) ... }  <----one sample
     * Output: { (gene_3, 0.689), (gene_4, 0.888), ... } <-- only return those that are > NORMAL_EXP_VAL
    */
    public DataBag exec(Tuple input) throws IOException {
        try{
            String gene_name = "";
            // The pig script is inputting a bag of tuples
            DataBag bag = (DataBag)input.get(0);
            Iterator it = bag.iterator();

            DataBag gene_bag = bagFactory.newDefaultBag();
            int count = 0;
            while(it.hasNext()) {
                count++;
                Tuple next = (Tuple)it.next();

                double next_val = Double.parseDouble(next.get(0).toString());
                if( next_val > NORMAL_EXP_VAL ) {
                    gene_name = "gene_" + String.valueOf(count);
                    Tuple gene = tupleFactory.newTuple(gene_name);
                    gene.append(next_val);
                    gene_bag.add(gene);
                }

            }
            return gene_bag;
        } catch (Exception e) {
            throw new IOException( "Caught exception processing input row " + e.getMessage(), e );
        }
    }
}
