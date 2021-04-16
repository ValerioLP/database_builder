package utility;

public class Year
{
    private int year;

    public Year(int year) throws IllegalArgumentException
    {
        if (year > 9999 || year < 1000)
            throw new IllegalArgumentException("l'anno inserito non è consentito");
        this.year = year;
    }

    public int getYear() { return year; }

    public boolean isBisestile() { return year % 4 == 0; }

    @Override
    public String toString() { return "" + year; }


}