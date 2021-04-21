package data;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */
public abstract class DataType
{
    /**
     * Campi della classe:
     */
    private String name;

    private boolean numeric;

    /**
     * costruttore della classe che crea un data type
     * @param name nome del tipo
     * @param numeric true se il tipo è numerico, false altrimenti
     */
    public DataType(String name, boolean numeric)
    {
        this.name = name;
        this.numeric = numeric;
    }

    /**
     * metodo che controlla se un data type è numerico
     * @return true se il data type è numerico, false altrimenti
     */
    public boolean isNumeric() { return numeric; }

    /**
     * metodo che crea un'istanza random del randomize
     * @return una stringa randomizzata deldata type
     */
    public abstract String randomize();

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