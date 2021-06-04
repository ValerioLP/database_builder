package data;

import utility.Month;

import java.util.Random;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */
public class Time extends DataType
{
    public Time() { super("time", false); }

    @Override
    public String randomize() {
        Random r = new Random();
        return (r.nextInt(23) + 1) + ":" + (r.nextInt(59) + 1) + ":" + (r.nextInt(59) + 1) + "." + (r.nextInt(9999998) + 1);
    }
}