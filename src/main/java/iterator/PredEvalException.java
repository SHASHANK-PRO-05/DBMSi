package iterator;

import chainexception.ChainException;

public class PredEvalException extends ChainException {
  public PredEvalException(Exception e, String message) {
    super(e, message);
  }
}
