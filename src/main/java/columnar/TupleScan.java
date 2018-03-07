package columnar;

import global.TID;
import heap.Tuple;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class TupleScan {
  private ColumnarFile cf;

  TupleScan(ColumnarFile cf) {
    this.cf = cf;
  }

  void closeTupleScan() {
    throw new NotImplementedException();
  }

  Tuple getNext(TID tid) {
    throw new NotImplementedException();
  }

  boolean position(TID tid) {
    throw new NotImplementedException();
  }
}
