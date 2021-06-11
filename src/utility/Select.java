package utility;

import db.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Classe utilizzata per creare delle query di selezione dalle tabelle.
 * Sfruttata principalmente per essere data in pasto al metodo select della classe Database.
 * @author Luca Mattei, Valerio Mezzoprete
 */
public final class Select extends Query {
	
    public static class QueryBuilder {
        /**
         * Campi della classe builder:
         */
        static final String SELECT = "select ";

        private String query;
        private String orderBy;
        private String where;
        private String having;

        private List<String> attributes = new ArrayList<>();
        private List<String> tableNames = new ArrayList<>();
        private List<String> groupBys = new ArrayList<>();

        private int maxRow;

        /**
         * costruttore generale della classe builder
         * @param attributes lista di attributi della select
         */
        public QueryBuilder(String... attributes) { this(Arrays.asList(attributes)); }

        /**
         * costruttore specifico della classe builder
         * @param attributes array di attributi della select
         */
        public QueryBuilder(Collection<String> attributes) { attributes.forEach(a -> addAttribute(a)); }

        /**
         * metodo privato che aggiunge un'attributo tra quelli da selezionare nella query di select
         * @param attribute attributo da selezionare
         */
        private void addAttribute(String attribute)
        {
            attribute = attribute.toLowerCase();
            if (!attributes.contains(attribute))
                attributes.add(attribute);
        }

        /**
         * metodo che aggiunge un table name alla query di select
         * @param tableName nome del table incluso nella query
         * @return l'istanza del query builder
         */
        public QueryBuilder addTable(String tableName)
        {
            tableName = tableName.toLowerCase();
            if (!tableNames.contains(tableName))
                tableNames.add(tableName);
            return this;
        }

        /**
         * metodo che aggiunge un attributo a quelli su cui fare il group by nella query
         * @param attribute nome dell'attributo da aggiungere
         * @return l'istanza del query builder
         */
        public QueryBuilder addGroupBy(String attribute)
        {
            attribute = attribute.toLowerCase();
            if (!groupBys.contains(attribute))
                groupBys.add(attribute);
            return this;
        }

        /**
         * metodo che aggiunge un attributo su cui ordinare
         * @param attribute nome dell'attributo su cui ordinare
         * @return l'istanza del query builder
         */
        public QueryBuilder addOrderBy(String attribute) throws IllegalArgumentException
        {
            if (orderBy != null)
                throw new IllegalArgumentException("puoi selezionare un solo parametro su cui ordinare");
            orderBy = attribute.toLowerCase();
            return this;
        }

        /**
         * metodo che aggiunge un limite alle righe da visualizzare
         * @param maxRow numero massimo di righe da visualizzare
         * @return l'istanza del query builder
         */
        public QueryBuilder limit(int maxRow) throws IllegalArgumentException
        {
            if (maxRow != 0)
                throw new IllegalArgumentException("hai gia settato il numero di righe massime per la query");
            this.maxRow = maxRow;
            return this;
        }

        /**
         * metodo che aggiunge tutte le condizioni di where alla query
         * @param conditions stringa formattata con il where della query
         * @return l'istanza del query builder
         */
        public QueryBuilder addWhere(String conditions) throws IllegalArgumentException
        {
            if (where != null)
                throw new IllegalArgumentException("hai gia inserito la clausula where");
            where = conditions.toLowerCase();
            return this;
        }

        /**
         * metodo che aggiunge tutte le condizioni all'having della query
         * @param conditions stringa formattata con l'having della query
         * @return l'istanza del query builder
         */
        public QueryBuilder addHaving(String conditions) throws IllegalArgumentException
        {
            if (having != null)
                throw new IllegalArgumentException("hai gia inserito la clausula where");
            having = conditions.toLowerCase();
            return this;
        }

        /**
         * builder della classe che dopo aver fatto i controlli sulla query la crea
         * @return l'istanza della query costruita
         */
        public Query build()
        {
            if (tableNames.size() == 0)
                throw new IllegalArgumentException("la select prevede almeno una tabella per eseguire la query con la clausula from");

            //cominciamo a costruire la query sottoforma di stringa
            StringBuilder query = new StringBuilder(SELECT);

            //per ogni attributo della tabella aggiungiamo alla query la corrispettiva stringa
            attributes.forEach(a -> query.append(a + ", "));
            query.delete(query.length() -2, query.length()).append(" from ");
            tableNames.forEach(t -> query.append(t + ", "));
            query.delete(query.length() -2, query.length());
            if (where != null)
                    query.append(" where " + where);
            if (groupBys.size() > 0) {
                query.append(" group by ");
                groupBys.forEach(g -> query.append(g + ", "));
                query.delete(query.length() -2, query.length());
            }
            if (having != null)
                query.append(" having " + having);
            if (orderBy != null)
                query.append(" order by " + orderBy);
            if (maxRow > 0)
                query.append(" limit " + maxRow);

            this.query = query.toString();

            return new Select(this);
        }
    }
    
	
    /**
     * Costruttore della classe Select che salva la query generata dal builder
     * @param builder query builder
     */
    private Select(QueryBuilder builder) { super(builder.query); }

}
