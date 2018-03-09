package columnar;

import global.TID;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;

public class TupleScan {
  private ColumnarFile cf;
  private short columnNo;
  private Scan scans[];
  private int noOfColumns;

  TupleScan(ColumnarFile cf) throws IOException, InvalidTupleSizeException {
    this.cf = cf;

    this.noOfColumns = this.cf.heapFileNames.length;
    this.scans = new Scan[noOfColumns];

    for (int i = 0; i < noOfColumns; i++) {
      this.scans[i] = new Scan(this.cf, (short) i);
    }
  }

  void closeTupleScan() {
    for (int i = 0; i < noOfColumns; i++) {
      this.scans[i].closeScan();
    }
  }

  Tuple getNext(TID tid) throws InvalidTupleSizeException, IOException {
    Tuple nextTuples[] = new Tuple[noOfColumns];

    for (int i = 0; i < noOfColumns; i++) {
      nextTuples[i] = scans[i].getNext(tid.recordIDs[i]);
    }

    return mergeTuples(nextTuples);
  }

  boolean position(TID tid) throws InvalidTupleSizeException, IOException {
    boolean result = true;

    for (int i = 0; i < noOfColumns; i++) {
      result = scans[i].position(tid.recordIDs[i]);
    }

    return result;
  }

  private Tuple mergeTuples(Tuple tuples[]) {
    Tuple reslTuple = new Tuple(tuples[0]);

    for (int i = 1; i < noOfColumns; i++) {
      reslTuple.mergeTuple(tuples[i]);
    }

    return reslTuple;
  }
}
