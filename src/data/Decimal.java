package data;

public final class Decimal extends DataType
{
    private int decimals;

    private int precision;

    public Decimal(int decimals, int precision)
    {
        super("decimal", true);
        this.decimals = decimals;
        this.precision = precision;
    }

    @Override
    public String toString() { return super.toString() + "(" + decimals + ", " + precision + ")"; }
}