package query;

import java.util.TreeMap;

/**
 * Classe utilizzata per creare delle query di update.
 * Sfruttata principalmente per essere data in pasto al metodo  della classe Database.
 * @author Luca Mattei, Valerio Mezzoprete
 */
public class Update extends Query {

    public static class QueryBuilder {
        /**
         * Campi della classe builder:
         */
        static final String UPDATE = "update ";

        private String tableName;
        private String query;
        private String where;

        private TreeMap<String, String> attributes = new TreeMap<>();

        /**
         * costruttore della classe builder
         * @param tableName table nel quale va fatto l'update
         */
        public QueryBuilder(String tableName) {
            this.tableName  = tableName;
        }

        /**
         * modifica nel db il valore inserito all'attributo inserito
         * @param attributeName nome dell'attributo da modificare
         * @param value valore da sostuire al vecchio
         * @return l'istanza del query builder
         */
        public Update.QueryBuilder addValue(String attributeName, String value) {
            attributes.put(attributeName, value);
            return this;
        }

        /**
         * aggiunge la condizione di where
         * @param conditions
         * @return
         */
        public Update.QueryBuilder addWhere(String conditions)
        {
            where = conditions;
            return this;
        }

        /**
         * builder della classe che dopo aver fatto i controlli sulla query la crea
         * @return l'istanza della query costruita
         */
        public Query build() throws IllegalArgumentException {
            if (attributes.size() == 0)
                throw new IllegalArgumentException("non sono stati inseriti attributi con i rispettivi valori");

            //cominciamo a costruire la query sottoforma di stringa
            StringBuilder query = new StringBuilder(UPDATE);
            query.append(tableName + " set ");

            //per ogni attributo della tabella aggiungiamo alla query la corrispettiva stringa
            attributes.forEach((k, v) -> query.append(k + " = \"" + v + "\", "));
            query.delete(query.length() -2, query.length());
            if (where != null)
                query.append(" where " + where);

            this.query = query.toString();

            return new Update(this);
        }
    }

    /**
     * Costruttore della classe Update che salva la query generata dal builder
     * @param builder query builder
     */
    private Update(Update.QueryBuilder builder) { super(builder.query); }
}
