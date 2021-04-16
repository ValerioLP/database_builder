package db;

public class Vincolo {
	/**
	 * Campi della classe:
	 */
    private String vincolato;
    private String referencedTable;
    private String foreignKey;

    /**
     * Costruttore della classe
     * @param vincolato nome dell'attributo vincolato della tabella su cui si sta inserendo il vincolo
     * @param referencedTable tabella referenziata da cui andare a prendere la foreign key da vincolare
     * @param foreignKey da vincolare all'attributo
     * @throws IllegalArgumentException controlla se la foreign key è presente o meno nella tabella
     * referenziata.
     */
    public Vincolo(String vincolato, Table referencedTable, String foreignKey) throws IllegalArgumentException {
        if (!referencedTable.checkAttribute(foreignKey))
            throw new IllegalArgumentException("la foreign key fornita non e' presente nella tabella");
        this.vincolato = vincolato;
        this.referencedTable = referencedTable.getName();
        this.foreignKey = foreignKey;
    }

    /**
     * metodi getter delle parti di cui è formato il vincolo.
     * @return le varie parti del vincolo
     */
    public String getVincolato() { return vincolato; }
    public String getReferencedTable() { return referencedTable; }
    public String getForeignKey() { return foreignKey; }
}