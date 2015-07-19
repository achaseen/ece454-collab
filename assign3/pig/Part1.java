import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import java.util.Iterator;
import org.apache.hadoop.io.WritableComparable;

public class Part1 extends EvalFunc<String> {
    public String exec(Tuple input) throws IOException {
        try{
            String max_gene_list = "";
            double max_gene = 0;

            // The pig script is inputting a bag of tuples
            DataBag bag = (DataBag)input.get(0);
            Iterator it = bag.iterator();

            int count = 0;
            while(it.hasNext()) {
                count++;
                Tuple next = (Tuple)it.next();
                // The tuples that are passed in have only 1 entry, which is the gene value
                // Ridiculous hack to get DataBag->Tuple->String->Double
                double next_val = Double.parseDouble(next.get(0).toString());
                if( next_val > max_gene ) {
                    max_gene = next_val;
                    max_gene_list = "gene_" + String.valueOf(count);
                } else if (next_val == max_gene) {
                    // If there are two genes that are both max, add it to the list
                    max_gene_list = max_gene_list + ",gene_" + String.valueOf(count);
                }

            }
            return max_gene_list;
        } catch (Exception e) {
            throw new IOException( "Caught exception processing input row " + e.getMessage(), e );
        }
    }
}
