package db;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */
public class Table {
	/**
	 * Classe builder interna alla classe Table che crea istanze di tabelle	 
	 */
    public static class TableBuilder {
    	
    	/**
    	 * Campi della classe builder:
    	 */
        private String name;

        private List<Attribute> attributes = new ArrayList<>();

        private List<Vincolo> vincoli = new ArrayList<>();

        /**
         * costruttore del table builder
         * @param name prende in input il nome della tabella che si vuole creare
         */
        public TableBuilder(String name) { this.name = name.toLowerCase(); }

        /**
         * mettodo che serve per aggiungere un attributo all'interno della tabella che si
         * sta creando.
         * @param attribute prende in input un attributo da aggiungere all'istanza della tabella
         * che si sta creando.
         * @return l'istanza del table builder
         */
        public TableBuilder addAttribute(Attribute attribute) {
            attributes.add(attribute);
            return this;
        }

        /**
         * metodo che aggiunge un vincolo legato ad un attributo della tabella con l'attributo di
         * un'altra tabella.
         * @param vincolo prende in input un vincolo, formato dall'attributo vincolato, la tabella
         * referenziata e la foreign key
         * @return ritorna l'istanza del table builder
         * @throws IllegalArgumentException controlla se l'attributo vincolato è presente o no nella
         * tabella
         */        
        public TableBuilder addVincolo(Vincolo vincolo)throws IllegalArgumentException {        	
            if (!attributes.stream().anyMatch(x -> x.getName().equals(vincolo.getVincolato())))
            throw new IllegalArgumentException("l'attributo vincolato non e' presente nella tabella");
            this.vincoli.add(vincolo);
            return this;
        }

        /**
         * metodo terminale build che controlla se è stata inserita la primary key nella tabella
         * @return ritorna l'istanza della tabella finale creata con il builder.
         */
        public Table build() {
            if (!attributes.stream().anyMatch(x -> x.isKey()))
                throw new IllegalArgumentException("la tabella " + name + " non ha primary key");
            return new Table(this);
        }
    }

    /**
     * Campi della classe:
     */
    private static final String INTRO = "create table if not exists ";

    private String name;

    private List<Attribute> attributes = new ArrayList<>();
    private List<Vincolo> vincoli = new ArrayList<>();

    /**
     * Costruttore della classe Table che salva i parametri costruiti nel table builder
     * @param builder prende in input l'istanza del table builder costruita
     */
    private Table(TableBuilder builder) {
        name = builder.name;
        attributes.addAll(builder.attributes);
        vincoli.addAll(builder.vincoli);
    }
    
    /**
     * metodo che controlla se la stringa data in input corrispondente ad un attributo
     * sia presente nella lista degli attributi della tabella
     * @param attribute stringa corrispondente al nome di un attributo ricercato
     * @return true se la tabella ha un attributo con nome che corrisponde alla stringa, false altrimenti
     */
    public boolean checkAttribute(String attribute) {
        return attributes.stream()
                .anyMatch(x -> x.getName().equals(attribute));
    }

    /**
     * metodo getter
     * @return restituisce il nome della tabella
     */
    public String getName() { return name; }
    
    /**
     * metodo getter
     * @param attributeName prende in input il nome di un attributo
     * @return l'istanza dell'attributo inserito se esiste, null altrimenti
     */
    public Attribute getAttribute(String attributeName) {
        return attributes.stream()
                .filter(x -> x.getName().equals(attributeName))
                .reduce((x, y) -> x)
                .orElse(null);
    }

	/**
	 * metodo getter
	 * @return ritorna una collection di tutti i vincoli salvati per la tabella
	 */
    public Collection<Vincolo> getVincoli() { return vincoli; }
    
    /**
     * metodo terminale build che controlla se c'è piu' di un attributo settato con auto incremental,
     * o se ci sono attributi con nomi uguali, o se non è stata inserita nessuna chiave nella tabella
     * @throws IllegalArgumentException se piu di un attributo ha settato l'auto increment a true
     * oppure i nomi degli attributi non sono univoci
     * oppure non sono state inserite chiavi nella tabella
     */
    public void build() throws IllegalArgumentException {
        if ((int)attributes.stream().filter(x -> x.getAutoIncremental()).count() > 1)
            throw new IllegalArgumentException("piu' di un attributo ha settato l'auto increment a true");
        
        Set<String> set = attributes.stream()
                .map(x -> x.getName())
                .collect(Collectors.toSet());
        if (set.stream().count() != attributes.stream().count())
            throw new IllegalArgumentException("i nomi degli attributi non sono univoci");
        
        if((int)attributes.stream().filter(x -> x.isKey()).count() == 0)
            throw new IllegalArgumentException("non e' stata inserita nessuna chiave nella tabella");
    }

    /**
     * metodo getter 
     * @return ritorna la query fatta a stringa che verrà lanciata in mySQL
     */
    public String getQuery() {
    	//inizio a scrivere la query
        StringBuilder out = new StringBuilder(INTRO + name + "  (");   
        //aggiungo gli attributi della tabella nella query
        attributes.forEach(x -> out.append(x.getQuery() + ", "));
        //inizio a scrivere la/le primary key
        out.append("primary key (");
        //controllo le chiavi e le scrivo nella query
        attributes.stream()
                .filter(x -> x.isKey())
                .forEach(x -> out.append(x.getName() + ", "));        
        out.delete(out.length()-2, out.length());
        out.append("), ");
        //aggiungo gli unique
        attributes.stream()
                .filter(x -> x.isUnique())
                .forEach(x -> out.append("unique index " + x.getName() 
                + "_unique (" + x.getName() + " ASC) visible, "));
       //controllo se ci sono vincoli e li aggiungo nella query
        if (!vincoli.isEmpty())
            vincoli.forEach(x -> out.append("constraint " + x.getVincolato() + " foreign key (" +
                    x.getVincolato() + ") references " + x.getReferencedTable() + " ("
                    + x.getForeignKey() + ") on delete no action on update no action, "));
        out.delete(out.length() - 2, out.length());
        //ritorno la query sottoforma di stringa
        return out.append(");").toString();
    }

    @Override
    public String toString() {
        StringBuilder out = new StringBuilder(name + " (");
        attributes.forEach(x -> out.append(x + ", "));
        out.delete(out.length()-2, out.length());
        return out.append(")").toString();
    }
}