package utility;

import db.Database;
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

        private Database db;

        private TreeMap<String, String> attributes = new TreeMap<>();

        /**
         * costruttore della classe builder
         * @param db database
         * @param tableName nome del table
         */
        public QueryBuilder(Database db, String tableName) {
            this.tableName  = tableName;
            this.db = db;
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
         * @throws IllegalArgumentException se la tabella o gli attributi inseriti non sono presenti nel db
         * oppure se ci sono attibuti obbligatori non inseriti
         */
        public Query build() throws IllegalArgumentException {
            Table t = db.getTable(tableName.toLowerCase());

            //controlliamo che la tabella e gli attributi passati in input sono presenti nel db
            if (t == null) { throw new IllegalArgumentException("la tabella inserita non è presente nel db"); }
            if (!attributes.keySet()
                    .stream()
                    .allMatch(x -> t.getAttribute(x) == null ?  false : t.getAttribute(x).getName().equals(x)))
                throw new IllegalArgumentException("uno o piu degli attributi inseriti non è presente nel table");
            if (!t.getAttributes()
                    .stream()
                    .filter(x -> x.isNotNull())
                    .filter(x -> !x.getAutoIncremental())
                    .allMatch(x -> attributes.keySet().contains(x.getName())))
                throw new IllegalArgumentException("uno o piu attributi obbligatori non sono stati inseriti nella lista");

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
