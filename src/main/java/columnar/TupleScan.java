package columnar;

import global.TID;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;
import columnar.ByteToTuple;

import java.io.IOException;
import java.util.Arrays;

public class TupleScan {
    private ColumnarFile cf;
    private Scan scans[];
    private int columnNos[];

    /**
     * Create a Tuple scan over all the columns in a Columnar file
     *
     * @param cf Columnar File to be scanned for Tuples
     * @throws IOException
     * @throws InvalidTupleSizeException
     */
    public TupleScan(ColumnarFile cf) throws IOException, InvalidTupleSizeException {
        this.cf = cf;

        int noOfColumns = this.cf.getHeapFileNames().length;

        columnNos = new int[noOfColumns];

        for (int i = 0; i < noOfColumns; i++) {
            columnNos[i] = i;
        }

        scans = new Scan[noOfColumns];

        for (int i = 0; i < noOfColumns; i++) {
            scans[i] = new Scan(this.cf, (short) i);
        }
    }

    /**
     * Create a tuple scan over only a particular column set
     *
     * @param cf             Columnar File to be scanned
     * @param columnNosArray Column IDs that are supposed to be scanned in Columnar File
     * @throws IOException
     * @throws InvalidTupleSizeException
     */
    public TupleScan(ColumnarFile cf, int columnNosArray[]) throws IOException, InvalidTupleSizeException {
        this.cf = cf;
        int noOfColumns = this.cf.getHeapFileNames().length;
        for (int i = 0; i < columnNosArray.length; i++) {
            if (columnNosArray[i] > noOfColumns) {
                throw new IOException("Error -> Column No: " + columnNosArray[i] + ", greater than available columns. " +
                        "The no of columns in the columnar file: " + cf.getHeapFileNames().length);
            }
        }

        noOfColumns = columnNosArray.length;
        columnNos = new int[noOfColumns];

        System.arraycopy(columnNosArray, 0, columnNos, 0, noOfColumns);

        scans = new Scan[noOfColumns];

        for (int i = 0; i < noOfColumns; i++) {
            scans[i] = new Scan(cf, (short) columnNosArray[i]);
        }
    }

    /**
     * Private function that returns the no of columns that make up the scanned tuple
     *
     * @return No of columns being scanned.
     */
    private int getNoOfColumns() {
        return this.columnNos.length;
    }

    /**
     * Close scanning on all the columns in the tuple scan.
     */
    public void closeTupleScan() {
        for (int i = 0; i < getNoOfColumns(); i++) {
            this.scans[i].closeScan();
        }
    }

    /**
     * Gets the next tuples from all the columns being scanned and merges them into a single tuple
     *
     * @param tid Tuple ID object
     * @return A single tuple that is the result of a merge between all the scanned column tuples
     * @throws InvalidTupleSizeException
     * @throws IOException
     */
    public Tuple getNext(TID tid) throws InvalidTupleSizeException, IOException {
        int noOfColumns = getNoOfColumns();
        ByteToTuple byteToTuple = new ByteToTuple();
        Tuple nextTuples[] = new Tuple[noOfColumns];
        int size = 0;
        for (int i = 0; i < noOfColumns; i++) {
            nextTuples[i] = scans[i].getNext(tid.getRecordIDs()[i]);
            if (nextTuples[i] == null) return null;
            	size += nextTuples[i].getLength();
        } 

        return byteToTuple.mergeTuples(nextTuples, size);
    }

    /**
     * Positions the scans on different columns to a particular tuple ID
     *
     * @param tid Tuple ID
     * @return returns true with the positioning is successful
     * @throws InvalidTupleSizeException
     * @throws IOException
     */
    public boolean position(TID tid) throws InvalidTupleSizeException, IOException {
        boolean result = true;

        for (int i = 0; i < getNoOfColumns(); i++) {
            result = scans[i].position(tid.getRecordIDs()[i]);
        }

        return result;
    }

}
