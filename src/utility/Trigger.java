package utility;

import db.Table;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */

public class Trigger extends Query {
    /**
     * Campi della classe trigger:
     */
    static final String TRIGGER = "create definer = current_user trigger ";

    private String triggerName;

    private Table table;

    /**
     * costruttore della classe trigger
     * @param table table al quale il trigger fa riferimento
     * @param triggerName nome del trigger
     * @param timing decide se fare l'azione prima o dopo
     * @param action azione da fare
     * @param body corpo del trigger
     */
    public Trigger(Table table, String triggerName, Timing timing, Action action, Granularity granularity, String body) throws IllegalArgumentException {
        super(TRIGGER + triggerName.toLowerCase() + " " + timing.toString().toLowerCase() + " " + action.toString().toLowerCase() +
                " on " + table.getName() + " for each " + granularity.toString().toLowerCase() + " begin " + body + " end");
        body.replace("\n", "");
        this.table = table;
        this.triggerName = triggerName;
    }



    //JAVADOC
    public String getTriggerName() { return triggerName; }

    public String getTableName() { return table.getName(); }
}
