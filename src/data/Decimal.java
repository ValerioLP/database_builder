package data;

import java.util.Random;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */
public final class Decimal extends DataType
{
    /**
     * cifre decimali prima della virgola
     */
    private int decimals;

    /**
     * cifre decimali dopo la virgola
     */
    private int precision;

    public Decimal(int decimals, int precision)
    {
        super("decimal", true);
        this.decimals = decimals;
        this.precision = precision;
    }

    @Override
    public String randomize() {
        Random r = new Random();
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < decimals; i++)
            out.append(r.nextInt(10));
        out.append(",");
        for (int i = 0; i < precision; i++)
            out.append(r.nextInt(10));
        return out.toString();
    }

    @Override
    public String toString() { return super.toString() + "(" + decimals + ", " + precision + ")"; }
}