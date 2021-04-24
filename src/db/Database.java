package db;

import data.Account;
import exceptions.DriverNotFoundException;
import exceptions.ForeignKeyException;
import utility.Insert;
import utility.Query;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/** 
 * @author Luca Mattei, Valerio Mezzoprete
 */
public class Database {
	
	/**
	 * Classe builder interna alla classe Database che crea istanze di database	 
	 */
    public static class DatabaseBuilder {
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
        public Database build() throws ForeignKeyException, SQLException, DriverNotFoundException {
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
            return db;
        }
    }

    /**
     * Campi della classe:
     */
    static final String DRIVERS = "com.mysql.cj.jdbc.Driver";
    static final String CREATE = "create database if not exists ";
    static final String USE = "use ";

    private String name;
    private String url;    
    private String query;

    Connection conn = null;

    private List<Table> tables = new ArrayList<>();

    /**
     * costruttore della classe Database. Salva il nome del database, l'url del server mySQL
     * e l'account dell'utente mySQL inseriti nel DatabaseBuilder. 
     * Viene utilizzato nel metodo build della classe DatabaseBuilder
     * @param builder prende un DatabaseBuilder in input
     */
    private Database(DatabaseBuilder builder) throws SQLException, DriverNotFoundException {
        name = builder.name;
        url = builder.url;
        tables.addAll(builder.tables);
        query = CREATE + name + ";\n" + USE + name + ";\n";

        System.out.println("loading drivers...");

        //prova a caricare i drivers
        try { Class.forName(DRIVERS); }
        catch(ClassNotFoundException e) {
            throw new DriverNotFoundException("error occured during driver loading");
        }
        System.out.println("driver loaded");

        //prova a connettersi al database
        System.out.println("connecting to db...");

        try { conn = DriverManager.getConnection(url, builder.account.getUsername(), builder.account.getPassword()); }
        catch(SQLException e) {
            throw new SQLException("error occured during db connection");
        }
        System.out.println("connection established");

        //esegue le query in sql creando il database
        create();
    }
    
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
     * metodo getter
     * @return restituisce la query creata fino a questo momento
     */
    public String getQuery() { return query; }

    /**
     * metodo che esegue effettivamente la connessione al database ed esegue tutte le query 
     */
    private void create() {
        //per ogni tabella del database andiamo a creare la rispettiva query in stringa
        // dando vita a una lunga stringa fatta da tutte le query da eseguire in mySQL
        tables.forEach(x -> query += x.getQuery() + ";\n");
            
        //prima di eseguirle tutte come unica stringa...
        //splittiamo le query per poterle eseguire una alla volta e non tutte insieme.
        String[] queries = query.split("\n");

        //andiamo ad eseguire una per volta tutte le query salvate nell'array
        for (int i = 0; i < queries.length; i++) {
            try { executeQuery(queries[i]); }
            catch(SQLException e) { e.printStackTrace(); }
            int k = i + 1;
            System.out.println("query #" + k + " eseguita correttamente");
        }
    }
    
    /**
     * metodo di utilità generico privato che esegue la query in input
     * @param query la query sottoforma di stringa
     * @throws SQLException
     */
    private void executeQuery(String query) throws SQLException {
        //prova a creare una connessione con mySQL per permettere l'utilizzo delle query
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            System.out.println(query);
            stmt.execute(query);
        }
        catch(SQLException e) {
            throw new SQLException("error occured during query execution");
        }
    }

    /**
     * popola il db con entry casuali
     */
    public void randomPopulate() {
        //prendo la lista di table che ha almeno una foreign key
        List<Table> linkedTables = tables.stream()
                .filter(t -> !t.getVincoli().isEmpty())
                .collect(Collectors.toList());
        //prendo la lista di table che non ha foreign key
        List<Table> freeTables = tables.stream()
                .filter(t -> linkedTables.stream().map(x -> x.getName()).noneMatch(x -> x.equals(t.getName())))
                .collect(Collectors.toList());
        //costruisco una mappa da table.attributo a lista di attributi che sono foreign key in altre table
        Map<String, List<Attribute>> tableMap = new HashMap<>();

        linkedTables.stream()
                .forEach(t -> t.getVincoli().forEach(v -> {
                    String key = getTable(v.getReferencedTable()).getName() + "." + v.getForeignKey();
                    if (tableMap.containsKey(key))
                        tableMap.get(key).add(t.getAttribute(v.getVincolato()));
                    else
                    {
                        List<Attribute> l = new ArrayList<>();
                        l.add(t.getAttribute(v.getVincolato()));
                        tableMap.put(key, l);
                    }
                }));

        //print per test
        linkedTables.stream().forEach(x -> System.out.println(x));
        System.out.println("-------------------------");
        freeTables.stream().forEach(x -> System.out.println(x));
        System.out.println("-------------------------");
        tableMap.forEach((k, v) -> System.out.println(k + " : " + v.toString()));
        System.out.println();

        //costruisco una mappa da table.attribute a valori generati per quell'attributo
        Map<String, List<String>> valoriGenerati = new HashMap<>();

        //generiamo i valori e quelli vincolati li salviamo all'interno di una mappa
        freeTables.stream()
                .forEach(t -> {
                    for (int i = 0; i < 1000; i++)
                    {
                        Insert.QueryBuilder q = new Insert.QueryBuilder(this, t.getName());
                        t.getAttributes().stream().forEach(a -> {
                            //creo un valore random suldominio del tipo
                            String randomValue = a.getType().randomize();
                            //genero la chiave nel formato table.attribute
                            String key = t.getName() + "." + a.getName();
                            if (tableMap.containsKey(key))
                            {
                                //se la chiave è presente nella mappa allora aggiungo il valore all'insieme
                                valoriGenerati.computeIfPresent(key, (k, v) -> { v.add(randomValue); return v; } );
                                //se la chiave non è presente nella mappa allora creo un insieme e ci aggiungo il valore
                                valoriGenerati.computeIfAbsent(key, k -> {
                                    List<String> l = new LinkedList<>();
                                    l.add(randomValue);
                                    return l;
                                });
                            }
                            q.addValue(a.getName(), randomValue);
                        });
                        try { insert(q.build()); }
                        catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                });

        valoriGenerati.forEach((k, v) -> System.out.println(k + " : " + v.toString()));
        valoriGenerati.forEach((k, v) -> System.out.println(v.size()));


        //fixare problma di ordinamento delle tabelle linkate (sort topologico)


        Random random = new Random();
        linkedTables.stream()
                .forEach(t -> {
                    for (int i = 0; i < 1000; i++)
                    {
                        Insert.QueryBuilder q = new Insert.QueryBuilder(this, t.getName());
                        t.getAttributes().stream().forEach(a -> {
                            if (!a.getAutoIncremental()) {
                                String randomValue;

                                boolean insideMap = false;

                                for (String k : tableMap.keySet())
                                    insideMap = tableMap.get(k).contains(a);

                                if (insideMap) {
                                    //prendiamo la chiave corrispondente dalla mappa talbe.attribute -> [attributi vincolati]
                                    String key = "";
                                    for (String k : tableMap.keySet())
                                        if (tableMap.get(k).contains(a))
                                            key = k;

                                    List<String> listaValori = valoriGenerati.get(key);
                                    randomValue = listaValori.get(random.nextInt(listaValori.size()));
                                }
                                else
                                    randomValue = a.getType().randomize();
                                q.addValue(a.getName(), randomValue);
                            }
                        });
                        Query query = q.build();
                        try { insert(query); }
                        catch (SQLException e) {
                            System.out.println("errore nella query: " + query);
                            break;
                        }
                    }
                });
    }

    /**
     * metodo specifico per l'esecuzione di query di tipo insert
     * @param query l'istanza della query ObjOr
     * @throws SQLException 
     */
    public void insert(Query query) throws SQLException { executeQuery(query.toString()); }

    @Override
    public String toString() { return name + " " + url; }
}