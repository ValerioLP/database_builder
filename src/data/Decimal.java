package data;

import java.util.Random;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */
public final class Decimal extends DataType
{
    /**
     * numero totale di cifre decimali
     */
    private int digits;

    /**
     * cifre decimali dopo la virgola
     */
    private int precision;

    public Decimal(int digits, int precision) throws IllegalArgumentException
    {
        super("decimal", true);
        if (precision > digits)
            throw new IllegalArgumentException("il numero di cifre decimali dopo la virgola deve essere >= al numero di cifre decimali prima della virgola");
        this.digits = digits;
        this.precision = precision;
    }

    @Override
    public String randomize() {
        Random r = new Random();
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < digits - precision; i++)
            out.append(r.nextInt(10));
        out.append(".");
        for (int i = 0; i < precision; i++)
            out.append(r.nextInt(10));
        return out.toString();
    }

    @Override
    public String toString() { return super.toString() + "(" + digits + ", " + precision + ")"; }
}