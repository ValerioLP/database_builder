package data;

import utility.Month;

import java.util.Random;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */
public final class Date extends DataType
{
    public Date() { super("date", false); }

    @Override
    public String randomize() {
        Random r = new Random();
        Month mese = Month.randomMonth();
        return (r.nextInt(9) + 1) + (r.nextInt(9) + 1) + (r.nextInt(9) + 1) + (r.nextInt(9) + 1) + "-" +
                mese.getMonth() + "-" + (r.nextInt(mese.getMaxDay()) + 1);
    }
}