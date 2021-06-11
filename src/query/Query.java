package query;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */
public abstract class Query {
    /**
     * Campi della classe:
     */
    private String query;

    protected Query(String query) { this.query = query; }

    @Override
    public String toString() { return query; }
}