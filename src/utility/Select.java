package utility;

import db.*;

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
        
        private String tableName;
        private String query;

        private Database db;
        
        /**
         * costruttore della classe builder
         * @param db database
         * @param tableName nome del table
         */
        public QueryBuilder(Database db, String tableName) {
            this.tableName  = tableName;
            this.db = db;
        }
    }
    
	
    /**
     * Costruttore della classe Select che salva la query generata dal builder
     * @param builder query builder
     */
    private Select(QueryBuilder builder) { super(builder.query); }

}
