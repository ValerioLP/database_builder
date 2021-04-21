package data;

import java.util.Random;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */
public final class VarChar extends DataType
{
    private int characters;

    public VarChar(int characters)
    {
        super("varchar", false);
        this.characters = characters;
    }

    @Override
    public String randomize(){
        String s = "abcdefghijklmnopqrstuvwxyz";
        Random r = new Random();
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < characters; i++)
            out.append(s.charAt(r.nextInt(s.length())));
        return out.toString();
    }

    @Override
    public String toString() { return super.toString() + "(" + characters + ")"; }
}