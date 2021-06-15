package db;

import data.Account;
import exceptions.DriverNotFoundException;
import exceptions.ForeignKeyException;
import utility.Coppia;
import query.Insert;
import query.Query;
import utility.MyConsumer;

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
        private List<Trigger> triggers = new ArrayList<>();

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
         *  metodo che aggiunge un trigger al db
         * @param trigger trigger da aggiungere al table
         * @return l'istanza del table builder
         */
        public DatabaseBuilder addTrigger(Trigger trigger) throws IllegalArgumentException
        {
            if (triggers.stream().anyMatch(t -> t.getTriggerName().equals(trigger.getTriggerName())))
                throw new IllegalArgumentException("il nome dei trigger deve essere univoco");
            triggers.add(trigger);
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
    private static final String DRIVERS = "com.mysql.cj.jdbc.Driver";
    private static final String CREATE = "create database if not exists ";
    private static final String USE = "use ";

    private int autoIncremental = 1;
    private int queryCounter = 1;
    private int duplicateEntryCounter = 0;

    private String name;
    private String url;
    private String query;

    Connection conn = null;

    private List<Table> tables = new ArrayList<>();
    private List<Trigger> triggers = new ArrayList<>();

    /**
     * campi con delle funzioni consumer che verranno utilizzati nei metodi di population
     */
    private final MyConsumer<Map<String, List<String>>, Set<String>, Table, Integer> GENERIC_CONSUMER = (valoriGenerati, attributiDaSalvare, t, n) ->
    {
        List<Attribute> attributes = t.getAttributes();

        //per ogni inserimento da fare
        for (int j = 0; j < n; j++)
        {
            //costruiamo la query di inserimento
            Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());

            //iteriamo su tutti gli attributi
            for (Attribute a : attributes)
            {
                if (!a.getAutoIncremental()) //se l'attributo non è autoincremental
                    notAutoIncrementalCase(t, attributiDaSalvare, valoriGenerati, q, a);
                else //l'attributo è autoincremental
                    autoIncrementalCase(t, attributiDaSalvare, valoriGenerati, a);
            } //chiusura del for sugli attributi
            executeQuery(q.build());
        }//chiusura del for sugli inserimenti
    };

    private final MyConsumer<Map<String, List<String>>, Set<String>, Table, Integer> MISSION_CONSUMER = (valoriGenerati, attributiDaSalvare, t, n) ->
    {
        //per ogni inserimento da fare
        for (int j = 0; j < n; j++)
        {
            //costruiamo la query di inserimento
            Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());

            Random r = new Random();

            //inseriamo l'id missione
            String key = t.getName() + ".id";
            //se la chiave è presente nella mappa allora aggiungo il valore all'insieme
            valoriGenerati.computeIfPresent(key, (k, v) -> {
                v.add(autoIncremental + "");
                return v;
            });
            //se la chiave non è presente nella mappa allora creo un insieme e ci aggiungo il valore
            valoriGenerati.computeIfAbsent(key, k ->
            {
                List<String> l = new LinkedList<>();
                l.add(autoIncremental + "");
                return l;
            });
            autoIncremental++;

            //inseriamo la regione
            List<String> regioniGenerate = valoriGenerati.get("regione.nome");
            q.addValue("regione", regioniGenerate.get(r.nextInt(regioniGenerate.size())));

            //inseriamo il tipo di missione
            String tipoMissione = t.getAttribute("tipo_missione").getType().randomize();
            q.addValue("tipo_missione", tipoMissione);

            if (tipoMissione.equals("assegnazione") || tipoMissione.equals("taglia")) //tipo assegnazione o taglia
            {
                //inseriamo tutti gli attributi
                q.addValue("grado_richiesto", t.getAttribute("grado_richiesto").getType().randomize());

                q.addValue("ricompensa", t.getAttribute("ricompensa").getType().randomize());

                q.addValue("obiettivo", t.getAttribute("obiettivo").getType().randomize());

                q.addValue("lv_difficolta", t.getAttribute("lv_difficolta").getType().randomize());

                q.addValue("nome", t.getAttribute("nome").getType().randomize());

                q.addValue("descrizione", t.getAttribute("descrizione").getType().randomize());

                q.addValue("tempo_limite", t.getAttribute("tempo_limite").getType().randomize());

                q.addValue("numero_vite", t.getAttribute("numero_vite").getType().randomize());

                q.addValue("npc", t.getAttribute("npc").getType().randomize());

                if (tipoMissione.equals("assegnazione")) //se è solo di tipo assegnazione
                    q.addValue("tipo_assegnazione", t.getAttribute("tipo_assegnazione").getType().randomize());
            }
            executeQuery(q.build());
        }//fine del for su gli inserimenti
    };

    private final MyConsumer<Map<String, List<String>>, Set<String>, Table, Integer> WEAPON_CONSUMER = (valoriGenerati, attributiDaSalvare, t, n) ->
    {
        //per ogni inserimento da fare
        for (int j = 0; j < n; j++)
        {
            //costruiamo la query di inserimento
            Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());

            //OBBLIGATORI nome, tipo, attacco, affinita
            //OPZIONALI

            Random r = new Random();

            //prendiamo il nome da equipaggiamento e lo salviamo tra i valori salvati
            Set<String> equipaggiamentiGeneratiSet = new HashSet<>(valoriGenerati.get("equipaggiamento.nome"));
            equipaggiamentiGeneratiSet.removeAll(valoriGenerati.get("armatura.nome"));
            List<String> equipaggiamentiGenerati = new ArrayList<>(equipaggiamentiGeneratiSet);

            String equipaggiamento = equipaggiamentiGenerati.get(r.nextInt(equipaggiamentiGenerati.size()));
            String key = t.getName() + ".nome";
            //se la chiave è presente nella mappa allora aggiungo il valore all'insieme
            valoriGenerati.computeIfPresent(key, (k, v) -> {
                v.add(equipaggiamento);
                return v;
            });
            //se la chiave non è presente nella mappa allora creo un insieme e ci aggiungo il valore
            valoriGenerati.computeIfAbsent(key, k ->
            {
                List<String> l = new LinkedList<>();
                l.add(equipaggiamento);
                return l;
            });

            //randomizzo e inserisco nel db i valori obbligatori
            q.addValue("nome", equipaggiamento);
            q.addValue("attacco", t.getAttribute("attacco").getType().randomize());
            q.addValue("affinita", t.getAttribute("affinita").getType().randomize());


            //randomizzo se l'arma ha o meno una difesa
            int randomInt = r.nextInt(100);
            if (randomInt < 50) //ha una difesa
                q.addValue("difesa", t.getAttribute("difesa").getType().randomize());

            //randomizzo l'arma e in base al tipo aggiungi i valori opzionali necessari
            String weaponType = t.getAttribute("tipo").getType().randomize();

            if (!weaponType.equals("arco") && !weaponType.substring(0,4).equals("bale")) //se è un'arma da taglio avrà un'acutezza
            {
                q.addValue("acutezza",  t.getAttribute("acutezza").getType().randomize());

                if (weaponType.equals("lancia fucile")) // è una lancia fucile
                {
                    q.addValue("tipo_proiettile", t.getAttribute("tipo_proiettile").getType().randomize());
                    q.addValue("lv_proiettile", t.getAttribute("lv_proiettile").getType().randomize());
                }
                else if (weaponType.equals("spadascia") || weaponType.equals("lama caricata"))
                    q.addValue("tipo_fiala", t.getAttribute("tipo_fiala").getType().randomize());
            }
            else if (!weaponType.equals("arco"))//è una balestra
            {
                q.addValue("rinculo", t.getAttribute("rinculo").getType().randomize());
                if (weaponType.equals("balestra pesante")) //è una balestra pesante
                    q.addValue("proiettile_speciale", t.getAttribute("proiettile_speciale").getType().randomize());
            }

            if (!weaponType.substring(0,4).equals("bale"))
            {
                //randomizzo uno tra elemento/status/nessuno dei due
                randomInt = r.nextInt(100);
                if (randomInt < 33) //ha elemento
                {
                    //prendo un elemento casuale tra quelli gia inseriti
                    List<String> elementiGenerati = valoriGenerati.get("elemento.nome");
                    String elemento = elementiGenerati.get(r.nextInt(elementiGenerati.size()));

                    q.addValue("elemento", elemento);
                    q.addValue("attacco_elementale", t.getAttribute("attacco_elementale").getType().randomize());
                }
                else if (randomInt < 66) //ha status
                {
                    //prendo uno status casuale tra quelli gia inseriti
                    List<String> statusGenerati = valoriGenerati.get("status.nome");
                    String status = statusGenerati.get(r.nextInt(statusGenerati.size()));

                    q.addValue("status", status);
                    q.addValue("attacco_status", t.getAttribute("attacco_status").getType().randomize());
                }
            }

            q.addValue("tipo", weaponType);
            executeQuery(q.build());
        }//fine del for su gli inserimenti
    };

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
        triggers.addAll(builder.triggers);
        query = CREATE + name + "\n" + USE + name + "\n";

        //prova a caricare i drivers
        System.out.println("loading drivers...");
        try {
            Class.forName(DRIVERS);
            System.out.println("driver loaded");
        }
        catch(ClassNotFoundException e) {
            throw new DriverNotFoundException("error occured during driver loading");
        }

        //prova a connettersi al database
        System.out.println("connecting to db...");
        try {
            conn = DriverManager.getConnection(url, builder.account.getUsername(), builder.account.getPassword());
            System.out.println("connection established");
        }
        catch(SQLException e) {
            throw new SQLException("error occured during db connection");
        }

        //esegue le query in sql creando il database
        create();
    }

    /**
     * Costruttore privato della classe database
     * @param url del server a cui connettersi
     * @param databaseName nome del server a cui connettersi
     * @param account dell'utente a cui connettersi
     * @throws SQLException se non è stato possibile connettersi al db
     */
    private Database(String url, String databaseName, Account account) throws SQLException
    {
        this.url = url;
        this.name = databaseName;
        //prova a connettersi al database
        System.out.println("connecting to db...");
        try {
            conn = DriverManager.getConnection(url, account.getUsername(), account.getPassword());
            System.out.println("connection established");
        }
        catch(SQLException e) {
            throw new SQLException("error occured during db connection");
        }
        query = "use " + databaseName;
        executeQuery(query);
    }

    /**
     * metodo che ritorna la connessione al db passato in input
     * @param url del server a cui connettersi
     * @param databaseName nome del server a cui connettersi
     * @param account dell'utente a cui connettersi
     * @return l'istanza di connessione al db
     * @throws SQLException se non è stato possibile connettersi al db
     */
    public static Database connect(String url, String databaseName, Account account) throws SQLException
    {
        return new Database(url, databaseName, account);
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
        tables.forEach(x -> query += x.getQuery() + "\n");

        //per ogni trigger nel database andiamo a creare la rispettiva query in stringa
        //da aggiungere alla stringa creata sopra
        triggers.forEach(t -> query += t.toString() + "\n");

        //prima di eseguirle tutte come unica stringa...
        //splittiamo le query per poterle eseguire una alla volta e non tutte insieme.
        String[] queries = query.split("\n");

        //andiamo ad eseguire una per volta tutte le query salvate nell'array
        for (int i = 0; i < queries.length; i++) {
            executeQuery(queries[i]);

        }
    }
    
    /**
     * metodo di utilità generico privato che esegue la query in input
     * @param query la query sottoforma di stringa
     * @throws SQLException
     */
    private void executeQuery(String query) {
        //prova a creare una connessione con mySQL per permettere l'utilizzo delle query
        Statement stmt = null;
            try {
                stmt = conn.createStatement();
                System.out.println(query);

                queryCounter++;

                if (query.substring(0,6).equals("select")) {
                ResultSet out = stmt.executeQuery(query);
                System.out.println("query #" + queryCounter + " eseguita correttamente");

                ResultSetMetaData metaData = out.getMetaData();

                int columnCount = metaData.getColumnCount();

                //costruiamo l'output di una query
                StringBuilder queryOutput = new StringBuilder("+");

                int[] max_length = new int[columnCount];

                //per ogni colonna costruiamo la prima riga della query
                for (int i = 1; i <= columnCount  ; i++) {

                    int columnSize = metaData.getColumnDisplaySize(i);
                    max_length[i-1] = Math.max(columnSize, metaData.getColumnName(i).length());
                    queryOutput.append("-".repeat(max_length[i-1] + 2) + "+");
                }
                queryOutput.append("\n|");

                String primaRiga = queryOutput.substring(0,queryOutput.length()-1);

                //per ogni colonna costruiamo la seconda riga della query
                for (int i = 1; i <= columnCount  ; i++) {
                    String column_name = metaData.getColumnName(i);
                    queryOutput.append(" " + column_name + " ".repeat(max_length[i-1] - column_name.length()) + " |");
                }
                queryOutput.append("\n" + primaRiga);

                int countRows = 0;

                //per ogni riga della query e per ogni colonna inseriamo l'output
                while (out.next()) {
                    countRows++;
                    queryOutput.append("| ");
                    for (int i = 1; i <= columnCount  ; i++) {
                        String result = out.getString(i);
                            queryOutput.append(result + " ".repeat(max_length[i-1] - (result == null ? 4 : result.length())) + " | ");
                    }
                    queryOutput.append("\n");
                }

                if (countRows > 0)
                    queryOutput.append(primaRiga.substring(0, primaRiga.length()-1));

                System.out.println(queryOutput.toString());
                }
                else {
                stmt.execute(query);
                System.out.println("query #" + queryCounter + " eseguita correttamente");
                }
            }
            catch(SQLException e) {
                if (!e.getMessage().substring(0,9).equals("Duplicate"))
                    e.printStackTrace();
                else
                    duplicateEntryCounter++;
            }
    }

    /**
     * popola il db con entry casuali
     */
    public void randomPopulate() { randomPopulate(1000); }

    /**
     * metodo che ritorna il numero di duplicate entry che non sono state inserite nel db
     * @return numero di duplicate entry
     */
    public int getDuplicateEntryCounter() { return duplicateEntryCounter; }

    /**
     * popola il db con entry casuali
     * @param n numero di entry per table che verranno generate
     */
    public void randomPopulate(int n)
    {
        //dizionario che mappa ogni table a un numero
        Map<Table, Integer> tableToInt = new HashMap<>();

        //popoliamo la mappa
        for (int i = 0; i < tables.size(); i++)
            tableToInt.put(tables.get(i), i);

        Map<Integer, List<Coppia<Integer, String>>> graph = new HashMap();

        //popoliamo il grafo
        tables.stream().forEach(t -> graph.put(tableToInt.get(t), new ArrayList<>()));
        tables.stream()
                .forEach(t -> t.getVincoli().forEach(v -> {
                    graph.get(tableToInt.get(getTable(v.getReferencedTable()))).add(new Coppia(tableToInt.get(t), v.getForeignKey()));
                }));

        //facendo il sort topologico sul grafo otteniamo la lista ordinata delle table da popolare
        List<Integer> tableSortInt = sortTopologico(graph);
        List<Table> tableSort = tableSortInt.stream().map(i ->
        {
            for (Table t : tableToInt.keySet())
                if ((int)tableToInt.get(t) == i)
                    return t;
            return null;
        }).collect(Collectors.toList());

        //costruisco una mappa da table.attribute a valori generati per quell'attributo
        Map<String, List<String>> valoriGenerati = new HashMap<>();
        
        //iteriamo su ogni table in tableSort
        for (int i = 0; i < tableSort.size(); i++)
        {
            //generiamo un insieme di attributi che, nel momento in cui li creiamo vanno salvati
            Set<String> attributiDaSalvare = new HashSet<>();

            //popoliamo l'insieme
            for (Integer k : graph.keySet())
            {
                for (Coppia<Integer, String> c : graph.get(k))
                {
                    attributiDaSalvare.add(c.getSnd().toString());
                }
            }

            //popoliamo ogni table con n occorrenze
            for (int j = 0; j < n; j++)
            {
                Table t = tableSort.get(i);
                Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());

                t.getAttributes()
                        .stream()
                        .forEach(a ->
                        {
                            if (!a.getAutoIncremental()) //se l'attributo non è autoincremental
                            {
                                if (t.getVincoli().stream().noneMatch(v -> v.getVincolato().equals(a.getName()))) //caso in cui devo generare un valore casuale
                                {
                                    //creo un valore random sul dominio del tipo
                                    String randomValue = a.getType().randomize();

                                    //se l'attributo è contenuto nell'insieme degli attributi è da salvare
                                    if (attributiDaSalvare.contains(a.getName())) {
                                        //genero la chiave nel formato table.attribute
                                        String key = t.getName() + "." + a.getName();

                                        //se la chiave è presente nella mappa allora aggiungo il valore all'insieme
                                        valoriGenerati.computeIfPresent(key, (k, v) -> {
                                            v.add(randomValue);
                                            return v;
                                        });

                                        //se la chiave non è presente nella mappa allora creo un insieme e ci aggiungo il valore
                                        valoriGenerati.computeIfAbsent(key, k ->
                                        {
                                            List<String> l = new LinkedList<>();
                                            l.add(randomValue);
                                            return l;
                                        });
                                    }
                                    q.addValue(a.getName(), randomValue);
                                }
                                else //caso in cui devo prendere il valore dai valori generati
                                {
                                    Vincolo v = t.getVincoli().stream().
                                            filter(x -> x.getVincolato().equals(a.getName()))
                                            .reduce((x, y) -> x)
                                            .orElse(null);

                                    String key = v.getReferencedTable() + "." + v.getForeignKey();                                    

                                    List<String> listaValori = valoriGenerati.get(key);
                                    String randomValue = listaValori.get(new Random().nextInt(listaValori.size()));

                                    //se l'attributo è contenuto nell'insieme degli attributi è da salvare
                                    if (attributiDaSalvare.contains(a.getName())) {
                                        //genero la chiave nel formato table.attribute
                                        key = t.getName() + "." + a.getName();

                                        //se la chiave è presente nella mappa allora aggiungo il valore all'insieme
                                        valoriGenerati.computeIfPresent(key, (k, val) -> {
                                            val.add(randomValue);
                                            return val;
                                        });

                                        //se la chiave non è presente nella mappa allora creo un insieme e ci aggiungo il valore
                                        valoriGenerati.computeIfAbsent(key, k ->
                                        {
                                            List<String> l = new LinkedList<>();
                                            l.add(randomValue);
                                            return l;
                                        });
                                    }
                                    q.addValue(a.getName(), randomValue);
                                }
                            }
                            else //l'attributo è autoincremental
                            {
                                if (attributiDaSalvare.contains(a.getName())) //l'attributo è chiave
                                {
                                    String key = t.getName() + "." + a.getName();

                                    //se la chiave è presente nella mappa allora aggiungo il valore all'insieme
                                    valoriGenerati.computeIfPresent(key, (k, v) -> {
                                        v.add(autoIncremental + "");
                                        return v;
                                    });

                                    //se la chiave non è presente nella mappa allora creo un insieme e ci aggiungo il valore
                                    valoriGenerati.computeIfAbsent(key, k ->
                                    {
                                        List<String> l = new LinkedList<>();
                                        l.add(autoIncremental + "");
                                        return l;
                                    });
                                    autoIncremental++;
                                }
                            }
                        }); //chiusura del forEach
                executeQuery(q.build());
            }
            autoIncremental = 1;
        }
    }

    public void randomPopulateMHW(int n)
    {
        //table del db mhw ordinati tramite sort topologico
        String[] tableSort = new String[] {
                "fauna_endemica",
                "clima",
                "npc",
                "abilita",
                "mostro",
                "status",
                "status_clima",
                "status_mostro",
                "elemento",
                "elemento_mostro",
                "resistenza",
                "regione",
                "locazione",
                "clima_regione",
                "missione",
                "incontro",
                "gioiello",
                "ottenimento",
                "crafting",
                "ricetta",
                "richiesta",
                "ricavo",
                "proiettile",
                "rivestimento",
                "equipaggiamento",
                "creazione",
                "abilita_equipaggiamento",
                "armatura",
                "arma",
                "utilizzo_rivestimento",
                "utilizzo_proiettile",
                "set_equipaggiamento",
                "gioiello_equipaggiato",
                "armatura_equipaggiata",
                "account",
                "cacciatore",
                "set_posseduto",
                "missione_completata",
                "possedimento_gioiello",
                "possedimento_oggetto",
                "possedimento_equipaggiamento"
        };

        //costruisco una mappa da table.attribute a valori generati per quell'attributo
        Map<String, List<String>> valoriGenerati = new HashMap<>();

        //creiamo la lista di attributi da salvare
        Set<String> attributiDaSalvare = new HashSet<>();

        //iteriamo sui table per creare l'insieme di attributi da salvare
        for (int i = 0; i < tableSort.length; i++) {
            Table t = getTable(tableSort[i]);
            //inseriamo i vincoli tra gli attributi da salvare
            t.getVincoli().stream().forEach(v -> attributiDaSalvare.add(v.getReferencedTable() + "." + v.getForeignKey()));
        }

        //iteriamo su tutti i table
        for (int i = 0; i < tableSort.length; i++)
        {
            Table t = getTable(tableSort[i]);
            if (t.getName().equals("missione"))
                MISSION_CONSUMER.accept(valoriGenerati, attributiDaSalvare, t, n);
            else if (t.getName().equals("arma"))
                WEAPON_CONSUMER.accept(valoriGenerati, attributiDaSalvare, t, n);
            else
                GENERIC_CONSUMER.accept(valoriGenerati, attributiDaSalvare, t, n);

            autoIncremental = 1;
        }//chiusura del for sui table
    }

    /**
     * metodo privato che fa un ordinamento topologico su un grafo
     * @param graph
     * @return la lista dei nodi del grafo ordinati in modo topologico
     */
    private List<Integer> sortTopologico(Map<Integer, List<Coppia<Integer, String>>> graph)
    {
        int[] checked = new int[graph.size()];
        List<Integer> sol = new ArrayList<>(graph.size());
        for (int i = 0; i < graph.size(); i++)
        {
            if (checked[i] == 0)
            {
                DFS(i, graph, checked, sol);
            }
        }
        Collections.reverse(sol);
        return sol;
    }

    /**
     * metodo ricorsivo chiamato dal metodo di ordinamento topologico
     * @param x -> il nodo che si sta iterando
     * @param graph
     * @param checked -> lista dei nodi gia controllati
     * @param sol -> la lista finale dei nodi ordinati topologicamente
     */
    private void DFS(int x, Map<Integer, List<Coppia<Integer, String>>> graph, int[] checked, List<Integer> sol)
    {
        checked[x] = 1;
        for (Coppia<Integer, String> c : graph.get(x))
        {
            int y = (int) c.getFst();
            if (checked[y] == 0)
                DFS(y, graph, checked, sol);
        }
        sol.add(x);
    }

    /**
     * metodo specifico per l'esecuzione di query CRUD
     * @param query l'istanza della query ObjOr
     * @throws IllegalArgumentException se la tabella o gli attributi inseriti non sono presenti nel db
     * oppure se ci sono attibuti obbligatori non inseriti
     * @throws SQLException se la query non viene eseguita correttamente
     */
    public void executeQuery(Query query) {  executeQuery(query.toString()); }

    /**
     * Metodo per il caso in cui devo prendere il valore dai valori generati
     */
    private void getRandomValueGenerated(Table t, Set<String> attributiDaSalvare, Map<String, List<String>> valoriGenerati, Insert.QueryBuilder q, Attribute a)
    {
        Vincolo v = t.getVincoli().stream()
                .filter(x -> x.getVincolato().equals(a.getName()))
                .reduce((x, y) -> x)
                .orElse(null);

        String key = v.getReferencedTable() + "." + v.getForeignKey();

        List<String> listaValori = valoriGenerati.get(key);
        String randomValue = listaValori.get(new Random().nextInt(listaValori.size()));

        //genero la chiave nel formato table.attribute
        key = t.getName() + "." + a.getName();

        //se l'attributo è contenuto nell'insieme degli attributi è da salvare
        if (attributiDaSalvare.contains(key))
            checkAttribute(attributiDaSalvare, valoriGenerati, key, randomValue);
        q.addValue(a.getName(), randomValue);
    }

    /**
     * se l'attributo è contenuto nell'insieme degli attributi è da salvare
     */
    private void checkAttribute(Set<String> attributiDaSalvare, Map<String, List<String>> valoriGenerati, String key, String randomValue)
    {
        //se la chiave è presente nella mappa allora aggiungo il valore all'insieme
        valoriGenerati.computeIfPresent(key, (k, val) ->
        {
            val.add(randomValue);
            return val;
        });

        //se la chiave non è presente nella mappa allora creo un insieme e ci aggiungo il valore
        valoriGenerati.computeIfAbsent(key, k ->
        {
            List<String> l = new LinkedList<>();
            l.add(randomValue);
            return l;
        });
    }

    /**
     * Metodo per il caso in cui l'attributo non è autoincremental
     */
    private void notAutoIncrementalCase(Table t, Set<String> attributiDaSalvare, Map<String, List<String>> valoriGenerati, Insert.QueryBuilder q, Attribute a)
    {
        //caso in cui devo generare un valore casuale
        if (t.getVincoli().stream().noneMatch(v -> v.getVincolato().equals(a.getName())))
            randomValueGenerate(t, attributiDaSalvare, valoriGenerati, q, a);
        else //caso in cui devo prendere il valore dai valori generati
            getRandomValueGenerated(t, attributiDaSalvare, valoriGenerati, q, a);
    }

    /**
     * Metodo per il caso in cui l'attributo è autoincremental
     */
    private void autoIncrementalCase(Table t, Set<String> attributiDaSalvare, Map<String, List<String>> valoriGenerati, Attribute a)
    {
        String key = t.getName() + "." + a.getName();
        //se l'attributo è contenuto nell'insieme degli attributi è da salvare
        if (attributiDaSalvare.contains(key))
        {
            checkAttribute(attributiDaSalvare, valoriGenerati, key, "" + autoIncremental);
            autoIncremental++;
        }
    }

    /**
     * Metodo per il caso in cui devo generare un valore casuale
     */
    private void randomValueGenerate(Table t, Set<String> attributiDaSalvare, Map<String, List<String>> valoriGenerati, Insert.QueryBuilder q, Attribute a)
    {
        //creo un valore random sul dominio del tipo
        String randomValue = a.getType().randomize();

        //genero la chiave nel formato table.attribute
        String key = t.getName() + "." + a.getName();

        //se l'attributo è contenuto nell'insieme degli attributi è da salvare
        if (attributiDaSalvare.contains(key))
            checkAttribute(attributiDaSalvare, valoriGenerati, key, randomValue);
        q.addValue(a.getName(), randomValue);
    }

    @Override
    public String toString() { return name + " " + url; }
}