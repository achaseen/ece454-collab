import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import java.util.Iterator;
import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;
import java.util.StringTokenizer;

public class Part3 extends EvalFunc<DataBag>
{
	BagFactory bagFactory = BagFactory.getInstance();
	TupleFactory tupleFactory = TupleFactory.getInstance();

	public DataBag exec(Tuple input) throws IOException {
		try{
			DataBag gene_bag = bagFactory.newDefaultBag();
			Tuple gene = tupleFactory.newTuple();

			String s1 = input.get(0).toString();
			gene.append(s1);
			String index1 = s1.split("_")[1];
			gene.append(index1);
			String s2 = input.get(input.size()/2).toString();
			gene.append(s2);
			String index2 = s2.split("_")[1];
			gene.append(index2);

			double similarity = 0.00;

			for(int i = 1; i < input.size()/2; i++) {
				similarity += (Double.parseDouble(input.get(i).toString()) * Double.parseDouble(input.get(input.size()/2 + i).toString()));
			}
			gene.append(similarity);
			gene_bag.add(gene);

			return gene_bag;
		}
		catch(Exception e) {
			throw new IOException("Caught excpetion" + e.getMessage(),e);
		}
	}
}