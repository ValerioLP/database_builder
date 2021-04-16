package data;

public final class VarChar extends DataType
{
    private int characters;

    public VarChar(int characters)
    {
        super("varchar", false);
        this.characters = characters;
    }

    @Override
    public String toString() { return super.toString() + "(" + characters + ")"; }
}