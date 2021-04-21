package data;

import utility.Month;

import java.util.Random;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */
public final class DateTime extends DataType
{
    public DateTime() { super("datetime", false); }

    @Override
    public String randomize() {
        Random r = new Random();
        Month mese = Month.randomMonth();
        return (r.nextInt(9) + 1) + (r.nextInt(9) + 1) + (r.nextInt(9) + 1) + (r.nextInt(9) + 1) + "-" +
                mese.getMonth() + "-" + (r.nextInt(mese.getMaxDay()) + 1) +
                " " + r.nextInt(24) + ":" + r.nextInt(60) + ":" + r.nextInt(60);
    }
}