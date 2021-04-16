package db;

import data.Account;
import exceptions.ForeignKeyException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/** 
 * @author Luca Mattei, Valerio Mezzoprete
 */
public class Database {
	
	/**
	 * Classe builder interna alla classe Database che crea istanze di database	 
	 */
    public static class DatabaseBuilder    {
    	/**
    	 * Campi della classe builder:
    	 */
        private String name;
        private String url;
        private Account account;

        private List<Table> tables = new ArrayList<>();

        /**
         * Costruttore della classe builder che salva:
         * @param name il nome del database
         * @param url del server a cui connettersi
         * @param account dell'utente a cui connettersi
         */
        public DatabaseBuilder(String name, String url, Account account) {
            this.name = name.toLowerCase();
            this.url = url;
            this.account = account;
        }

        /**
         * metodo che aggiunge una tabella nella lista delle tabelle di cui si sta componendo lo schema
         * @param table prende una tabella in input e la aggiunge al db
         * @return l'istanza del builder
         */
        public DatabaseBuilder addTable(Table table) {
            tables.add(table);
            return this;
        }

        /**
         * metodo terminale che chiude la creazione del database
         * @return ritorna l'istanza del database buildato
         * @throws ForeignKeyException se tutti gli attributi vincolati sono compatibili
         */
        public Database build() throws ForeignKeyException {
            Database db = new Database(this);              
             //Stream che controlla se ogni vincolo di ogni tabella ha l'attributo vincolato 
             //uguale alla foreign key della tabella referenziata            
            if (!db.tables.stream()
                    .allMatch(t -> t.getVincoli()
                            .stream()
                            .allMatch(v -> t.getAttribute(v.getVincolato())
                            		.compatibleTo(db.getTable(v.getReferencedTable())
                            				.getAttribute(v.getForeignKey())))))
                throw new ForeignKeyException("I Vincoli sugli attributi inseriti possiedono opzioni differenti tra di loro. "
            			+ "I due attributi vincolati devono avere le stesse opzioni");           
            db.execute();
            return db;
        }
    }

    /**
     * Campi della classe:
     */
    static final String DRIVERS = "com.mysql.cj.jdbc.Driver";
    static final String CREATE = "create database if not exists ";
    static final String USE = "use ";

    private Account account;
    private String name;
    private String url;    
    private String query;

    private List<Table> tables = new ArrayList<>();

    /**
     * costruttore della classe Database. Salva il nome del database, l'url del server mySQL
     * e l'account dell'utente mySQL inseriti nel DatabaseBuilder. 
     * Viene utilizzato nel metodo build della classe DatabaseBuilder
     * @param builder prende un DatabaseBuilder in input
     */
    private Database(DatabaseBuilder builder)    {
        name = builder.name;
        url = builder.url;
        account = builder.account;
        tables.addAll(builder.tables);
        query = CREATE + name + ";\n" + USE + name + ";\n";
    }

    /**
     * metodo getter
     * @return restituisce la query creata fino a questo momento
     */
    public String getQuery() { return query; }
    
    /**
     * metoto getter
     * @param tableName prende un nome di una tabella in input
     * @return restituisce una istanza di tabella a partire dalla sua stringa se esiste, null altrimenti
     */
    public Table getTable(String tableName) {
    	return tables.stream()
    			.filter(x -> x.getName().equals(tableName))
    			.reduce((x, y) -> x)
    			.orElse(null);
    }

    /**
     * metodo che esegue effettivamente la connessione al database ed esegue tutte le query 
     */
    private void execute()    {
        System.out.println("loading drivers...");   
        
        //prova a caricare i drivers         
        try { Class.forName(DRIVERS); }
        catch(ClassNotFoundException e) {
            System.out.println("error occured during driver loading");
            e.printStackTrace();
        }
        System.out.println("driver loaded");
                
        //prova a connettersi al database         
        System.out.println("connecting to db...");
        Connection conn = null;
        try { conn = DriverManager.getConnection(url, account.getUsername(), account.getPassword()); }
        catch(SQLException e) {
            System.out.println("error occured during db connection");
            e.printStackTrace();
        }
        System.out.println("connection established");

        //prova a creare una connessione con mySQL per permettere l'utilizzo delle query        
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            
            //per ogni tabella del database andiamo a creare la rispettiva query in stringa
            //dando vita a una lunga stringa fatta da tutte le query da eseguire in mySQL             
            tables.forEach(x -> query += x.getQuery() + ";\n");
            System.out.println(query);
            
            //prima di eseguirle tutte come unica stringa...
            //splittiamo le query per poterle eseguire una alla volta e non tutte insieme.            
            String[] queries = query.split("\n");
                        
            //andiamo ad eseguire una per volta tutte le query salvate nell'array             
            for (int i = 0; i < queries.length; i++) {
                stmt.execute(queries[i]);
                int k = i+1;
                System.out.println("query #" + k + " eseguita correttamente");
            }
        }
        
        catch(SQLException e) {
            System.out.println("error occured during db building");
            e.printStackTrace();
        }
    }

    @Override
    public String toString() { return name + " " + url; }
}