package iterator;

import chainexception.ChainException;

public class UnknowAttrType extends ChainException {
  public UnknowAttrType(Exception prevException, String message) {
    super(prevException, message);
  }

  public UnknowAttrType(String message) {
    super(null, message);
  }
}
