package utility;

public class Day
{
    private int day;

    public Day(int day) throws IllegalArgumentException
    {
        if (day < 1 || day > 31)
            throw new IllegalArgumentException("il giorno inserito non è consentito");
        this.day = day;
    }

    public int getDay() { return day; }

    @Override
    public String toString() { return "" + day; }
}