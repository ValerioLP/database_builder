package utility;

import db.*;

import java.util.ArrayList;
import java.util.Arrays;
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
         * @param attributes array di attributi della select
         */
        public QueryBuilder(String[] attributes) {
            this(Arrays.asList(attributes));
        }

        /**
         * costruttore specifico della classe builder
         * @param attributes array di attributi della select
         */
        public QueryBuilder(List<String> attributes)
        {
            attributes.forEach(a -> addAttribute(a));
        }

        private void addAttribute(String attribute)
        {
            attribute = attribute.toLowerCase();
            if (!attributes.contains(attribute))
                attributes.add(attribute);
        }

        public QueryBuilder addTable(String tableName)
        {
            tableName = tableName.toLowerCase();
            if (!tableNames.contains(tableName))
                tableNames.add(tableName);
            return this;
        }

        public QueryBuilder addGroupBy(String attribute)
        {
            attribute = attribute.toLowerCase();
            if (!groupBys.contains(attribute))
                groupBys.add(attribute);
            return this;
        }

        public QueryBuilder addOrderBy(String attribute) throws IllegalArgumentException
        {
            if (orderBy != null)
                throw new IllegalArgumentException("puoi selezionare un solo parametro su cui ordinare");
            orderBy = attribute.toLowerCase();
            return this;
        }

        public QueryBuilder limit(int maxRow) throws IllegalArgumentException
        {
            if (maxRow != 0)
                throw new IllegalArgumentException("hai gia settato il numero di righe massime per la query");
            this.maxRow = maxRow;
            return this;
        }

        public QueryBuilder addWhere(String conditions) throws IllegalArgumentException
        {
            if (where != null)
                throw new IllegalArgumentException("hai gia inserito la clausula where");
            where = conditions.toLowerCase();
            return this;
        }

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
