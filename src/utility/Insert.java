package utility;

import db.Table;

import java.util.TreeMap;

/**
 * Classe utilizzata per creare delle query di inserimento.
 * Sfruttata principalmente per essere data in pasto al metodo insert della classe Database.
 * @author Luca Mattei, Valerio Mezzoprete
 */
public final class Insert extends Query {

    public static class QueryBuilder {
        /**
         * Campi della classe builder:
         */
        static final String INSERT = "insert into ";

        private String tableName;
        private String query;

        private TreeMap<String, String> attributes = new TreeMap<>();

        /**
         * costruttore della classe builder
         * @param tableName table nel quale va fatta l'insert
         */
        public QueryBuilder(String tableName) {
            this.tableName  = tableName;
        }

        /**
         * aggiunge al db il valore inserito all'attributo inserito
         * @param attributeName nome dell'attributo
         * @param value valore da inserire
         * @return l'istanza del query builder
         */
        public QueryBuilder addValue(String attributeName, String value) {
            attributes.put(attributeName, value);
            return this;
        }

        /**
         * builder della classe che dopo aver fatto i controlli sulla query la crea
         * @return l'istanza della query costruita
         */
        public Query build() {
            //cominciamo a costruire la query sottoforma di stringa
            StringBuilder query = new StringBuilder(INSERT);
            query.append(tableName + " (");

            //per ogni attributo della tabella aggiungiamo alla query la corrispettiva stringa
            attributes.forEach((k, v) -> query.append(k + ", "));
            query.delete(query.length() -2, query.length()).append(") values (");
            attributes.forEach((k, v) -> query.append("\"" + v + "\", "));
            query.delete(query.length() -2, query.length()).append(");");

            this.query = query.toString();

            return new Insert(this);
        }
    }

    /**
     * Costruttore della classe Insert che salva la query generata dal builder
     * @param builder query builder
     */
    private Insert(QueryBuilder builder) { super(builder.query); }
}
