package data;

public abstract class DataType
{
    private String name;

    private boolean numeric;

    public DataType(String name, boolean numeric)
    {
        this.name = name;
        this.numeric = numeric;
    }

    public boolean isNumeric() { return numeric; }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof DataType))
            return false;
        DataType dt = (DataType)o;
        return name.equals(dt.name) && ((numeric && dt.numeric) || (!numeric && !dt.numeric));
    }

    @Override
    public String toString()
    {
        return name;
    }
}