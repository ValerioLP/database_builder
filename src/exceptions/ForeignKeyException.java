package exceptions;

public class ForeignKeyException extends Exception {

	private static final long serialVersionUID = 1L;

	public ForeignKeyException(String stringa) { super(stringa); }
}
