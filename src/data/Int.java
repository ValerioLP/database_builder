package data;

import java.util.Random;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */
public final class Int extends DataType
{
    private int decimals;

    public Int() { this(9); }

    public Int(int decimals) {
        super("int", true);
        this.decimals = decimals;
    }

    @Override
    public String randomize() {
        StringBuilder out = new StringBuilder();
        Random r = new Random();
        for (int i = 0; i < decimals; i++)
            out.append(r.nextInt(10));
        return out.toString();
    }
}