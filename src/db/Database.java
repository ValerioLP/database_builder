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
    private int queryCounter = 0;
    private int duplicateEntryCounter = 0;

    private String name;
    private String url;
    private String query;

    Connection conn = null;

    private List<Table> tables = new ArrayList<>();
    private List<Trigger> triggers = new ArrayList<>();
    
    //----------------------------------------------------------CAMPI CONSUMER-----------------------------------------------------//

    /**
     * Inizio dei campi con delle funzioni consumer che verranno utilizzati nei metodi di population
     */
    
    /**
     * Consumer generico per insert senza problemi di trigger
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

    /**
     * Consumer sulla tabella missione
     */
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

    /**
     * Consumer sulla tabella arma
     */
    private final MyConsumer<Map<String, List<String>>, Set<String>, Table, Integer> WEAPON_CONSUMER = (valoriGenerati, attributiDaSalvare, t, n) ->
    {
        //prendiamo il nome da equipaggiamento e lo salviamo tra i valori salvati
        Set<String> equipaggiamentiGeneratiSet = new HashSet<>(valoriGenerati.get("equipaggiamento.nome"));
        equipaggiamentiGeneratiSet.removeAll(valoriGenerati.get("armatura.nome"));  
        //creo la lista degli equipaggiamenti gia esistenti nel database diversi da armature gia create
        List<String> equipaggiamentiGenerati = new ArrayList<>(equipaggiamentiGeneratiSet);
        
        //per ogni inserimento da fare
        for (int j = 0; j < n; j++)
        {
            //costruiamo la query di inserimento
            Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());

            Random r = new Random();
            
            String key = t.getName() + ".nome";
            
            //prendo un equipaggiamento random dalla lista degli equipaggiamenti gia esistenti
            String equipaggiamento = equipaggiamentiGenerati.get(r.nextInt(equipaggiamentiGenerati.size()));
            
            computeMap(valoriGenerati, key, equipaggiamento);

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
        }//fine del for sugli inserimenti
    };
    
    /**
     * Consumer sulla tabella richiesta
     */
    private final MyConsumer<Map<String, List<String>>, Set<String>, Table, Integer> RICHIESTA_CONSUMER = (valoriGenerati, attributiDaSalvare, t, n) ->
    {
    	//creo la lista delle ricette gia esistenti nel database
        List<String> ricetteGenerate = valoriGenerati.get("ricetta.id");
        //creo la lista degli oggetti gia esistenti
        List<String> oggettiGenerati = valoriGenerati.get("crafting.nome");
        //creo una mappa occorrenze
        Map<String, List<String>> occorrenze = new HashMap<>();
        
        //per ogni inserimento da fare
        for (int j = 0; j < n; j++)
        {
            //costruiamo la query di inserimento
            Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());
            
            /**
             * Prendo una ricetta gia esistente
             */
            //prendo una ricetta random dalla lista delle ricette gia esistenti
        	String ricetta = ricetteGenerate.get(new Random().nextInt(ricetteGenerate.size()));
        	
            /**
             * Prendo un oggetto_richiesto gia esistente
             */
            //prendo un oggetto random dalla lista degli oggetti gia esistenti
        	String oggetto = oggettiGenerati.get(new Random().nextInt(oggettiGenerati.size()));

        	//aggiungo l'oggetto alla ricetta
        	if(occorrenze.containsKey(ricetta)) 
        	{
        		if(!occorrenze.get(ricetta).contains(oggetto))
        		{        			
        			occorrenze.get(ricetta).add(oggetto); 
        		}
        		else
        			System.out.println("Questa query ha tentato di inserire nuovamente l'oggetto " + oggetto + " nella ricetta " + ricetta);
        	}
        	else 
        	{
        		List<String> oggetti = new ArrayList<>();
        		oggetti.add(oggetto);
        		occorrenze.put(ricetta, oggetti);
        	}            
            //se la ricetta ha meno di 2 oggetti da creazione richiesti posso inserire
            if(occorrenze.get(ricetta).size() < 3) 
            {
            	q.addValue("ricetta", ricetta);
            	q.addValue("oggetto_richiesto", oggetto); 
            	executeQuery(q.build());
            }
            else
            	System.out.println("Questa query ha tentato di inserire l'oggetto " + oggetto + " nella ricetta " + ricetta + " che possiede gia 2 occorrenze");
        }//fine del for sugli inserimenti
    };

    /**
     * Consumer sulla tabella rivestimento
     */
    private final MyConsumer<Map<String, List<String>>, Set<String>, Table, Integer> RIVESTIMENTO_CONSUMER = (valoriGenerati, attributiDaSalvare, t, n) ->
    {
        //creo la lista degli status gia inseriti
        List<String> statusGenerati = valoriGenerati.get("status.nome");
        //creo la lista degli oggetti generati
        Set<String> oggettiGeneratiSet = new HashSet<>(valoriGenerati.get("crafting.nome"));
        oggettiGeneratiSet.removeAll(valoriGenerati.get("proiettile.nome"));
        List<String> oggettiGenerati = new ArrayList<>(oggettiGeneratiSet);
        
        //per ogni inserimento da fare
        for (int j = 0; j < n; j++)
        {
            //costruiamo la query di inserimento
            Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());

            Random r = new Random();

            String name = oggettiGenerati.get(r.nextInt(oggettiGenerati.size()));

            computeMap(valoriGenerati, t.getName() + ".nome", name);

            q.addValue("nome", name);
            q.addValue("status", statusGenerati.get(r.nextInt(statusGenerati.size())));

            executeQuery(q.build());
        }//fine del for sugli inserimenti
    };

    /**
     * Consumer sulla tabella utilizzo_rivestimento
     */
    private final MyConsumer<Map<String, List<String>>, Set<String>, Table, Integer> UTILIZZO_RIVESTIMENTO_CONSUMER = (valoriGenerati, attributiDaSalvare, t, n) ->
    {
        //inizializziamo la lista degli archi
        List<String> listaArchi = new ArrayList<>();
        //creo la lista dei rivestimenti gia inseriti
        List<String> rivestimentiGenerati = valoriGenerati.get("rivestimento.nome");
        
        //creiamo uno statement e lo connettiamo al db
        Statement stmt = null;
        try { stmt = conn.createStatement(); }
        catch (SQLException e) {}

        try //prendiamo tutte le armi corrispondenti al tipo arco
        {
            ResultSet out = stmt.executeQuery("select nome from arma where tipo = \"arco\"");

            while(out.next())
                listaArchi.add(out.getString(1));
        }
        catch (SQLException e) { System.out.println("ERRORE DURANTE LA QUERY"); }
        
        Map<String, List<String>> rivestimentiMap = new HashMap<>();
        
        for (int j = 0; j < n; j++)
        {
            //costruiamo la query di inserimento
            Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());

            Random r = new Random();

            //stiamo attenti a controllare se sono state inserite o meno archi
            if(listaArchi.size() > 0) {
	            String arco = listaArchi.get(r.nextInt(listaArchi.size()));	            
	            String rivestimento = rivestimentiGenerati.get(r.nextInt(rivestimentiGenerati.size()));   
	            
	            if(rivestimentiMap.containsKey(arco))
	            	rivestimentiMap.get(arco).add(rivestimento);
	            else {
	        		List<String> rivestimentiList = new ArrayList<>();
	        		rivestimentiList.add(rivestimento);
	        		rivestimentiMap.put(arco, rivestimentiList);
	            }	
	            //buildo la query solo se l'arco selezionato ha meno di 4 rivestimenti gia equipaggiati
	            if(rivestimentiMap.get(arco).size() < 5) {
	            	//buildo la query
		            q.addValue("arma", arco);
		            q.addValue("rivestimento", rivestimento);
		            //la eseguo
		            executeQuery(q.build());
	            }	            	
            }
            else {
            	System.out.println("Non ci sono archi nella lista delle armi a cui poter inserire rivestimenti");
            	break;
            }
        }//fine del for sugli inserimenti        
    };
    
    /**
     * Consumer sulla tabella utilizzo_proiettile
     */
    private final MyConsumer<Map<String, List<String>>, Set<String>, Table, Integer> UTILIZZO_PROIETTILE_CONSUMER = (valoriGenerati, attributiDaSalvare, t, n) ->
    {
        //inizializziamo la lista delle balestre leggere e pesanti
        List<String> listaBalestre = new ArrayList<>();
        //creo la lista dei proiettili gia inseriti
        List<String> proiettiliGenerati = valoriGenerati.get("proiettile.nome");
        
        //creiamo uno statement e lo connettiamo al db
        Statement stmt = null;
        try { stmt = conn.createStatement(); }
        catch (SQLException e) {}

        try //prendiamo tutte le armi corrispondenti al tipo balestra leggera e pesante
        {
            ResultSet out = stmt.executeQuery("select nome from arma where tipo = \"balestra leggera\" or tipo = \"balestra pesante\"");

            while(out.next())
            	listaBalestre.add(out.getString(1));
        }
        catch (SQLException e) { System.out.println("ERRORE DURANTE LA QUERY"); }
        
        Map<String, List<String>> proiettiliMap = new HashMap<>();
        
        for (int j = 0; j < n; j++)
        {
            //costruiamo la query di inserimento
            Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());

            Random r = new Random();
            
            //stiamo attenti a controllare se sono state inserite o meno balestre
            if(listaBalestre.size() > 0) {
	            String balestra = listaBalestre.get(r.nextInt(listaBalestre.size()));	            
	            String proiettile = proiettiliGenerati.get(r.nextInt(proiettiliGenerati.size()));   
	            
	            if(proiettiliMap.containsKey(balestra))
	            	proiettiliMap.get(balestra).add(proiettile);
	            else {
	        		List<String> rivestimentiList = new ArrayList<>();
	        		rivestimentiList.add(proiettile);
	        		proiettiliMap.put(balestra, rivestimentiList);
	            }	
	            //buildo la query solo se la balestra selezionata ha meno di 4 proiettili gia equipaggiati
	            if(proiettiliMap.get(balestra).size() < 5) {
	            	//buildo la query
		            q.addValue("arma", balestra);
		            q.addValue("proiettile", proiettile);
		            //la eseguo
		            executeQuery(q.build());
	            }	            	
            }
            else {
            	System.out.println("Non ci sono balestre nella lista delle armi a cui poter inserire proiettili");
            	break;
            }
        }//fine del for sugli inserimenti        
    };
    
    /**
     * Consumer sulla tabella armatura_equipaggiata
     */
    private final MyConsumer<Map<String, List<String>>, Set<String>, Table, Integer> ARMATURA_EQUIPAGGIATA_CONSUMER = (valoriGenerati, attributiDaSalvare, t, n) ->
    {
    	Random r = new Random();
        List<String> elmiGenerati = new ArrayList<>();
        List<String> bustiGenerati = new ArrayList<>();
        List<String> parabracciaGenerati = new ArrayList<>();
        List<String> faldeGenerate = new ArrayList<>();
        List<String> gambaliGenerati = new ArrayList<>();
        //inizializziamo la lista dei set equipaggiamento gia generati
        List<String> setEquipaggiamento = valoriGenerati.get("set_equipaggiamento.id");
        //creo l'insieme dei set gia inseriti in armatura equipaggiata
        Set<String> setEstratti = new HashSet<>();
        
        //creiamo uno statement e lo connettiamo al db
        Statement stmt = null;
        try { stmt = conn.createStatement(); }
        catch (SQLException e) { System.out.println("ERRORE DURANTE LA CONNESSIONE"); }

        //prendiamo tutti i tipi di armatura
        try 
        {
            ResultSet out = stmt.executeQuery("select nome from armatura where tipo = \"elmo\"");
            while(out.next())
                elmiGenerati.add(out.getString(1));

            out = stmt.executeQuery("select nome from armatura where tipo = \"busto\"");
            while(out.next())
                bustiGenerati.add(out.getString(1));

            out = stmt.executeQuery("select nome from armatura where tipo = \"parabraccia\"");
            while(out.next())
                parabracciaGenerati.add(out.getString(1));

            out = stmt.executeQuery("select nome from armatura where tipo = \"falda\"");
            while(out.next())
                faldeGenerate.add(out.getString(1));

            out = stmt.executeQuery("select nome from armatura where tipo = \"gambali\"");

            while(out.next())
                gambaliGenerati.add(out.getString(1));
        }
        catch (SQLException e) { System.out.println("ERRORE DURANTE LA QUERY"); }    
        
        for (int j = 0; j < n; j++)
        {            
            String set = "";
            //controllo che il set non sia gia stato estratto
            do set = setEquipaggiamento.get(r.nextInt(setEquipaggiamento.size()));
            while(setEstratti.contains(set));
            //inserisco nell'insieme dei set estratti il set attuale
            setEstratti.add(set);
            
            if(elmiGenerati.size() > 0) {
            	String elmo = elmiGenerati.get(r.nextInt(elmiGenerati.size()));
                //costruiamo la query di inserimento
                Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());   
            	q.addValue("armatura", elmo);
            	q.addValue("set_equipaggiamento", set);
            	executeQuery(q.build());
            }
            if(bustiGenerati.size() > 0) {
            	String busto = bustiGenerati.get(r.nextInt(bustiGenerati.size()));
                //costruiamo la query di inserimento
                Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());   
            	q.addValue("armatura", busto);
            	q.addValue("set_equipaggiamento", set);
            	executeQuery(q.build());
            }
            if(parabracciaGenerati.size() > 0) {
            	String parabraccia = parabracciaGenerati.get(r.nextInt(parabracciaGenerati.size()));
                //costruiamo la query di inserimento
                Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());   
            	q.addValue("armatura", parabraccia);
            	q.addValue("set_equipaggiamento", set);
            	executeQuery(q.build());
            }
            if(faldeGenerate.size() > 0) {
            	String falda = faldeGenerate.get(r.nextInt(faldeGenerate.size()));
                //costruiamo la query di inserimento
                Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());   
            	q.addValue("armatura", falda);
            	q.addValue("set_equipaggiamento", set);
            	executeQuery(q.build());
            }
            if(gambaliGenerati.size() > 0) {
            	String gambali = gambaliGenerati.get(r.nextInt(gambaliGenerati.size()));
                //costruiamo la query di inserimento
                Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());   
            	q.addValue("armatura", gambali);
            	q.addValue("set_equipaggiamento", set);
            	executeQuery(q.build());
            }
        }//fine del for sugli inserimenti
    };

    /**
     * consumer sulle tabelle possedimento
     */
    private final MyConsumer<Map<String, List<String>>, Set<String>, Table, Integer> POSSEDIMENTO_CONSUMER = (valoriGenerati, attributiDaSalvare, t, n) ->
    {
        //inizializziamo la lista degli account
        List<String> listaAccount = new ArrayList<>();

        //creiamo uno statement e lo connettiamo al db
        Statement stmt = null;
        try { stmt = conn.createStatement(); }
        catch (SQLException e) {}

        try //prendiamo tutti gli account con almeno un cacciatore dal db
        {
            ResultSet out = stmt.executeQuery("select distinct account from cacciatore;");

            while(out.next())
                listaAccount.add(out.getString(1));
        }
        catch (SQLException e) { System.out.println("ERRORE DURANTE LA QUERY"); }

        List<Attribute> attributes = t.getAttributes();

        Random r = new Random();

        //per ogni inserimento da fare
        for (int j = 0; j < n; j++)
        {
            //costruiamo la query di inserimento
            Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());

            String account = listaAccount.get(r.nextInt(listaAccount.size()));

            //iteriamo su tutti gli attributi
            for (Attribute a : attributes)
            {
                if (a.getName().equals("cacciatore"))
                {
                    //facciamo una query di select per prendere i cacciatori appartenenti a quell'account

                    try
                    {
                        ResultSet out = stmt.executeQuery("select nome from cacciatore where account = \"" + account + "\"");

                        //ci salviamo l'output della query nella lista
                        List<String> nomiAccount = new ArrayList<>(3);

                        while(out.next())
                            nomiAccount.add(out.getString(1));

                        q.addValue("cacciatore", nomiAccount.get(r.nextInt(nomiAccount.size())));
                    }
                    catch (SQLException e) {}

                }
                if (a.getName().equals("account"))
                    q.addValue("account", account);

                if (!a.getName().equals("cacciatore") && !a.getName().equals("account")) //se l'attributo non è account o cacciatore
                    notAutoIncrementalCase(t, attributiDaSalvare, valoriGenerati, q, a);

            } //chiusura del for sugli attributi
            executeQuery(q.build());
        }//chiusura del for sugli inserimenti
    };

    /**
     * consumer sulla tabella cacciatore
     */
    private final MyConsumer<Map<String, List<String>>, Set<String>, Table, Integer> HUNTER_CONSUMER = (valoriGenerati, attributiDaSalvare, t, n) ->
    {
        //mappa che abina ad ogni account la lista dei suoi cacciatori
        Map<String, List<String>> accountCacciatori = new HashMap<>();

        List<String> listaAccount = valoriGenerati.get("account.id");

        listaAccount.forEach(a -> accountCacciatori.put(a, new ArrayList<>()));

        Random r = new Random();

        for (int j = 0; j < n; j++)
        {
            //costruiamo la query di inserimento
            Insert.QueryBuilder q = new Insert.QueryBuilder(t.getName());

            String account;

            do
                account = listaAccount.get(r.nextInt(listaAccount.size()));
            while (accountCacciatori.get(account).size() == 3);

            //genero un nome lo aggiungo alla query e ai valori generati

            String nome = t.getAttribute("nome").getType().randomize();
            computeMap(valoriGenerati, t.getName() + ".nome", nome);

            computeMap(accountCacciatori, account, nome);

            //inserico account e valori generati casualmente
            q.addValue("account", account);
            q.addValue("nome", nome);
            q.addValue("zenny", t.getAttribute("zenny").getType().randomize());
            q.addValue("grado", t.getAttribute("grado").getType().randomize());

            executeQuery(q.build());
        } //fine del for sugli inserimenti
    };
    	
    //-------------------------------------------------------CORPO DELLA CLASSE-----------------------------------------------------------------------//
    
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
        try
        {
            stmt = conn.createStatement();
            System.out.println(query);

            queryCounter++;

            if (query.substring(0,6).equals("select"))
            {
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
     * metodo specifico per l'esecuzione di query CRUD
     * @param query l'istanza della query ObjOr
     * @throws IllegalArgumentException se la tabella o gli attributi inseriti non sono presenti nel db
     * oppure se ci sono attibuti obbligatori non inseriti
     * @throws SQLException se la query non viene eseguita correttamente
     */
    public void executeQuery(Query query) {  executeQuery(query.toString()); }

    /**
     * popola il db con entry casuali
     */
    public void randomPopulate() { randomPopulate(1000); }

    /**
     * metodo che ritorna il numero di duplicate entry che non sono state inserite nel db
     * @return numero di duplicate entry
     */
    public int getDuplicateEntryCounter() { return duplicateEntryCounter; }

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
            else if (t.getName().equals("richiesta"))
                RICHIESTA_CONSUMER.accept(valoriGenerati, attributiDaSalvare, t, n);
            else if (t.getName().equals("rivestimento"))
                RIVESTIMENTO_CONSUMER.accept(valoriGenerati, attributiDaSalvare, t, n);
            else if (t.getName().equals("cacciatore"))
                HUNTER_CONSUMER.accept(valoriGenerati, attributiDaSalvare, t, n);
            else if (t.getName().substring(0,3).equals("pos") || t.getName().equals("set_posseduto") || t.getName().equals("missione_completata"))
                POSSEDIMENTO_CONSUMER.accept(valoriGenerati, attributiDaSalvare, t, n);
            else if (t.getName().equals("utilizzo_rivestimento"))
            	UTILIZZO_RIVESTIMENTO_CONSUMER.accept(valoriGenerati, attributiDaSalvare, t, n);
            else if (t.getName().equals("utilizzo_proiettile"))
            	UTILIZZO_PROIETTILE_CONSUMER.accept(valoriGenerati, attributiDaSalvare, t, n);
            else if (t.getName().equals("armatura_equipaggiata"))
            	ARMATURA_EQUIPAGGIATA_CONSUMER.accept(valoriGenerati, attributiDaSalvare, t, n);            
            else
                GENERIC_CONSUMER.accept(valoriGenerati, attributiDaSalvare, t, n);

            autoIncremental = 1;
        }//chiusura del for sui table
    }

    
    //--------------------------------------------------------METODI PER IL RANDOM POPULATE-----------------------------------------------//
    
    
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

        Map<Integer, List<Coppia<Integer, String>>> graph = new HashMap<Integer, List<Coppia<Integer, String>>>();

        //popoliamo il grafo
        tables.stream().forEach(t -> graph.put(tableToInt.get(t), new ArrayList<>()));
        tables.stream()
                .forEach(t -> t.getVincoli().forEach(v -> {
                    graph.get(tableToInt.get(getTable(v.getReferencedTable()))).add(new Coppia<Integer, String>(tableToInt.get(t), v.getForeignKey()));
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
                        	//se l'attributo non è autoincremental
                            if (!a.getAutoIncremental())
                            	notAutoIncrementalCase(t, attributiDaSalvare, valoriGenerati, q, a);
                            else //l'attributo è autoincremental
                            	autoIncrementalCase(t, attributiDaSalvare, valoriGenerati, a);                            
                        }); //chiusura del forEach
                executeQuery(q.build());
            }
            autoIncremental = 1;
        }
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
    
    
    //-----------------------------------------------METODI PRIVATI PER I CONSUMER-------------------------------------------------//
    
    
    /**
     * se l'attributo è contenuto nell'insieme degli attributi è da salvare
     */
    private void computeMap(Map<String, List<String>> valoriGenerati, String key, String randomValue)
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
     * Metodo per il caso in cui devo generare un valore casuale
     */
    private void randomValueGenerate(Table t, Set<String> attributiDaSalvare, Map<String, List<String>> valoriGenerati, Insert.QueryBuilder q, Attribute a)
    {
        //creo un valore random sul dominio del tipo
        String randomValue = a.getType().randomize();

        //genero la chiave nel formato table.attribute
        String key = t.getName() + "." + a.getName();

        //se l'attributo è contenuto nell'insieme degli attributi da salvare è quindi da salvare
        if (attributiDaSalvare.contains(key))
            computeMap(valoriGenerati, key, randomValue);
        q.addValue(a.getName(), randomValue);
    }
    
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
            computeMap(valoriGenerati, key, randomValue);
        q.addValue(a.getName(), randomValue);
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
            computeMap(valoriGenerati, key, "" + autoIncremental);
            autoIncremental++;
        }
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

    @Override
    public String toString() { return name + " " + url; }
}