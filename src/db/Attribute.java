package db;

import data.DataType;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */
public class Attribute {

    public static class AttributeBuilder {
    	/**
    	 * Campi della classe builder:
    	 */
        private String name;

        private DataType type;

        private boolean key;
        private boolean notNull;
        private boolean unique;
        private boolean fill;
        private boolean autoIncremental;
        private boolean generated;
        private boolean unsigned;
        private boolean index;

        /**
         * Costruttore della classe builder che salva il nome dell'attributo costruito e il tipo di dato
         * @param name nome dell'attributo
         * @param type tipo di dato dell'attributo
         */
        public AttributeBuilder(String name, DataType type) {
            this.name = name.toLowerCase();
            this.type = type;
        }

        /**
         * metodo che aggiunge l'opzione not null all'attributo
         * @return l'istanza dell'attribute builder
         */
        public AttributeBuilder addNotNull() {
            notNull = true;
            return this;
        }

        /**
         * metodo che aggiunge l'opzione key e not null all'attributo
         * @return l'istanza dell'attribute builder
         */
        public AttributeBuilder addKey() {
            key = true;
            notNull = true;
            return this;
        }

        /**
         * metodo che aggiunge l'opzione unique all'attributo
         * @return l'istanza dell'attribute builder
         */
        public AttributeBuilder addUnique() {
            unique = true;
            index = true;
            return this;
        }

        /**
         * metodo che aggiunge l'opzione zero fill all'attributo
         * @return l'istanza dell'attribute builder
         */
        public AttributeBuilder addFill() {
            fill = true;
            unsigned = true;
            return this;
        }

        /**
         * metodo che aggiunge l'opzione auto incremental all'attributo
         * @return l'istanza dell'attribute builder
         */
        public AttributeBuilder addAutoIncremental() {
            autoIncremental = true;
            unique = true;
            return this;
        }

        /**
         * metodo che aggiunge l'opzione generated all'attributo
         * @return l'istanza dell'attribute builder
         */
        public AttributeBuilder addGenerated() {
            generated = true;
            return this;
        }

        /**
         * metodo terminale build che controlla se ad un tipo numerico e stata inserita l'opzione zero fill,
         * se sono stati inseriti auto incremental e generated contemporaneamente 
         * e se e stata inserita l'opzione auto incremental a un tipo numerico
         * @return l'istanza dell'attributo creato
         */
        public Attribute build() {
            if (!type.isNumeric() && fill)
                throw new IllegalArgumentException("un tipo non numerico non puo avere l'opzione fill");
            if (autoIncremental && generated)
                throw new IllegalArgumentException("non si puo avere auto incremental e generated contemporaneamente");
            if (!this.type.isNumeric() && autoIncremental)
                throw new IllegalArgumentException("un tipo non numerico non puo avere l'opzione auto incremental");
            return new Attribute(this);
        }
    }

    /**
     * Campi della classe:
     */
    private String name;

    private DataType type;

    private boolean key;
    private boolean notNull;
    private boolean unique;
    private boolean fill;
    private boolean autoIncremental;
    private boolean generated;
    private boolean unsigned;
    private boolean index;

    /**
     * Costruttore della classe attribute che salva tutte le opzioni inserite dal builder 
     * @param builder
     */
    private Attribute(AttributeBuilder builder) {
        name = builder.name;
        type = builder.type;
        key = builder.key;
        notNull = builder.notNull;
        unique = builder.unique;
        fill = builder.fill;
        autoIncremental = builder.autoIncremental;
        generated = builder.generated;
        unsigned = builder.unsigned;
        index = builder.index;
    }

    /**
     * metodo che controlla se l'attributo è chiave
     * @return true se l'attributo è chiave, false altrimenti
     */
    public boolean isKey() { return key; }

    /**
     * metodo che controlla se l'attributo è indicizzato
     * @return true se l'attributo è indicizzato, false altrimenti
     */
    public boolean hasIndex() { return index; }

    /**
     * metodo che controlla se l'attributo è not null
     * @return true se l'attributo è not null, false altrimenti
     */
    public boolean isNotNull() { return notNull; }

    /**
     * metodo che controlla se l'attributo è unique
     * @return true se l'attributo è unique, false altrimenti
     */
    public boolean isUnique() { return unique; }

    /**
     * metodo che controlla se l'auto incremental è stato settato
     * @return true se l'attributo è chiave, false altrimenti
     */
    public boolean getAutoIncremental() { return autoIncremental; }

    /**
     * metodo che ritorna il nome dell'attributo
     * @return nome dell'attributo
     */
    public String getName() { return name; }

    /**
     * metodo che ritorna il tipo di dato dell'attributo
     * @return DataType dell'attributo
     */
    public DataType getType() { return type; }

    public void setIndex() { index = true; };

    /**
     * metodo che ritorna la query dell'attributo sottoforma di stringa
     * @return la query sottoforma di stringa dell'attributo creato.
     */
    public String getQuery() {
    	//inizio a scrivere la query inserendo il nome dell'attributo e il suo tipo
        StringBuilder out = new StringBuilder(name + " " + type.toString() + " ");
        //controllo se ha le varie opzioni e nel caso le aggiungo alla query
        if (fill)
            out.append("zerofill ");
        if (notNull)
            out.append("not null ");
        else
            out.append("null ");
        if (autoIncremental)
            out.append("auto_increment");
        //ritorno la query sottoforma di stringa
        return out.toString();
    }

    /**
     * metodo che controlla se l'attributo dato in input e compatibile con l'attributo
     * su cui viene applicato il metodo.
     * Esso controlla se sono dello stesso tipo e se entrambi hanno le stesse opzioni di
     * not null e zero fill
     * @param a attributo da controllare
     * @return true se a è compatibile con this, false altrimenti
     */
    public boolean compatibleTo(Attribute a) {
        return type.equals(a.type) &&
                ((notNull && a.notNull) || (!notNull && !a.notNull)) &&
                ((fill && a.fill) || (!fill && !a.fill));
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Attribute))
            return false;        
        Attribute a = (Attribute)o;
        return compatibleTo(a) && name.equals(a.name) &&
                ((autoIncremental && a.autoIncremental) || (!autoIncremental && !a.autoIncremental)) &&
                ((key && a.key) || (!key && !a.key)) &&
                ((unique && a.unique) || (!unique && !a.unique)) &&
                ((generated && a.generated) || (!generated && !a.generated));
    }

    @Override
    public String toString()
    {
        StringBuilder out = new StringBuilder(name + " " + type);
     
        if (key)
            out.append(" PK");
        if (notNull)
            out.append(" NN");
        if (unique)
            out.append(" UQ");
        if (unsigned)
            out.append(" UN");
        if (fill)
            out.append(" ZF");
        if (autoIncremental)
            out.append(" AI");
        if (generated)
            out.append(" G");
        
        return out.toString();
    }
}