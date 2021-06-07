package data;

import java.util.Random;

public class BigInt extends DataType
{
    private int decimals;

    public BigInt() { this(12); }

    public BigInt(int decimals) {
        super("bigint", true);
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
