package exceptions;

public class DriverNotFoundException extends Exception {

    private static final long serialVersionUID = 1L;

    public DriverNotFoundException(String stringa) { super(stringa); }

    public DriverNotFoundException() { this(""); }
}
