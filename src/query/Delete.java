package query;

import java.util.TreeMap;

/**
 * Classe utilizzata per creare delle query di cancellazione dalle tabelle
 * Sfruttata principalmente per essere data in pasto al metodo executeQuery della classe Database
 * @author Luca Mattei, Valerio Mezzoprete
 */
public final class Delete extends Query {

    private static final String DELETE = "delete from ";

    /**
     * costruttore della classe delete che prende in input table dal quale cancellare e condizione di eliminazione
     * @param tableName nome dal quale cancellare una o piu occorrenze
     * @param where condizione per la quale ogni occorrenza viene cancellata
     */
    public Delete(String tableName, String where) { super(DELETE + tableName + " where " + where); }
}
