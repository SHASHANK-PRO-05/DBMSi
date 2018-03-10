package columnar;

import global.TID;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;

import java.io.IOException;

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
  TupleScan(ColumnarFile cf) throws IOException, InvalidTupleSizeException {
    this.cf = cf;

    int noOfColumns = this.cf.heapFileNames.length;

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
  TupleScan(ColumnarFile cf, int columnNosArray[]) throws IOException, InvalidTupleSizeException {
    this.cf = cf;

    for (int i = 0; i < columnNosArray.length; i++) {
      if (columnNosArray[i] > cf.heapFileNames.length) {
        throw new IOException("Error -> Column No: " + columnNosArray[i] + ", greater than available columns. " +
            "The no of columns in the columnar file: " + cf.heapFileNames.length);
      }
    }

    int noOfColumns = columnNosArray.length;
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
  void closeTupleScan() {
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
  Tuple getNext(TID tid) throws InvalidTupleSizeException, IOException {
    int noOfColumns = getNoOfColumns();

    Tuple nextTuples[] = new Tuple[noOfColumns];

    for (int i = 0; i < noOfColumns; i++) {
      nextTuples[i] = scans[i].getNext(tid.getRecordIDs()[i]);
    }

    return mergeTuples(nextTuples);
  }

  /**
   * Positions the scans on different columns to a particular tuple ID
   *
   * @param tid Tuple ID
   * @return returns true with the positioning is successful
   * @throws InvalidTupleSizeException
   * @throws IOException
   */
  boolean position(TID tid) throws InvalidTupleSizeException, IOException {
    boolean result = true;

    for (int i = 0; i < getNoOfColumns(); i++) {
      result = scans[i].position(tid.getRecordIDs()[i]);
    }

    return result;
  }

  /**
   * Private function that merges tuples from different columns into a single tuple
   *
   * @param tuples Tuples to be merged
   * @return Resultant tuple from the merge
   */
  private Tuple mergeTuples(Tuple tuples[]) {
    Tuple reslTuple = new Tuple(tuples[0]);

    for (int i = 1; i < getNoOfColumns(); i++) {
      reslTuple.mergeTuple(tuples[i]);
    }

    return reslTuple;
  }
}
