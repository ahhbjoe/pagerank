import java.io.IOException;

/**
 * The Driver class implements PageRank iteratively, with two steps in each iteration, i.e. UnitMultplication and
 * UnitSum.
 */
public class Driver {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        // Path of transition.txt
        String transitionMatrixPath = args[0];
        // Path of pagerank.txt
        String prMatrixPath = args[1];
        // Path of UnitMultiplication result
        String unitProductPath = args[2];
        // Number of iterations
        int iteration = Integer.parseInt(args[3]);

        for (int i = 0; i < iteration; i++) {
            String[] args0 = {transitionMatrixPath, prMatrixPath + i, unitProductPath + i};
            UnitMultiplication.main(args0);
            String[] args1 = {unitProductPath + i, prMatrixPath + (i + 1)};
            UnitSum.main(args1);
        }
    }
}
