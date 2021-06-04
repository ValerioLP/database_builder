package data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */
public final class Enum extends DataType
{
    List<String> values;

    public Enum(List<String> values) {
        super("enum", false);
        this.values = new ArrayList<>(values);
    }

    public Enum(String[] values) { this(Arrays.asList(values)); }

    @Override
    public String randomize() { return values.get(new Random().nextInt(values.size())); }

    @Override
    public String toString() {
        StringBuilder out = new StringBuilder("enum (");
        values.forEach(v -> out.append("\"" + v + "\", "));
        out.delete(out.length()-2, out.length());
        return out.append(") ").toString();
    }
}