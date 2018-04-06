package iterator;

import chainexception.ChainException;

public class NestedLoopException extends ChainException {
  public NestedLoopException(String s) {
    super(null, s);
  }
}
