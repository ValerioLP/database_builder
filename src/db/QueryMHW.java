package db;

import query.Insert;
import query.Query;
import query.Select;
import utility.Order;

public class QueryMHW
{
    public static Query OP1 = new Select.QueryBuilder("arma.tipo", "count(*)")
            .addTable("missione_completata")
            .addTable("set_equipaggiamento")
            .addTable("arma")
            .addWhere("missione_completata.set_utilizzato = set_equipaggiamento.id and set_equipaggiamento.arma = arma.nome")
            .addGroupBy("arma.tipo")
            .build();

    public static Query OP2 = new Select.QueryBuilder("t1.nome", "(t1.contatore + t2.contatore) as somma")
            .addTable("abilita_missioni_completate as t1")
            .addTable("abilita_missioni_completate as t2")
            .addWhere("t1.contatore <> t2.contatore and t1.nome = t2.nome")
            .addGroupBy("t1.nome")
            .union(new Select.QueryBuilder("nome", "contatore as somma")
                    .addTable("abilita_missioni_completate")
                    .addWhere("nome not in (select t1.nome from abilita_missioni_completate as t1, abilita_missioni_completate as t2 " +
                            "where t1.contatore <> t2.contatore and t1.nome = t2.nome)")
                    .addGroupBy("nome")
                    .addOrderBy("somma", Order.DESCENDING)
                    .limit(1)
                    .build())
            .build();

    public static Query OP3 = new Select.QueryBuilder("utilizzo_rivestimento.rivestimento as nome", "count(*) as count")
            .addTable("missione_completata")
            .addTable("set_equipaggiamento")
            .addTable("utilizzo_rivestimento")
            .addWhere("missione_completata.set_utilizzato = set_equipaggiamento.id and set_equipaggiamento.arma = utilizzo_rivestimento.arma")
            .addGroupBy("nome")
            .addOrderBy("count", Order.DESCENDING)
            .limit(1)
            .build();

    public static Query OP4 = new Select.QueryBuilder("nome", "attacco_elementale as valore_massimo")
            .addTable("arma")
            .addWhere("attacco_elementale = (select max(attacco_elementale) from arma where tipo = \"doppie lame\" and elemento = \"zzixtxvlvdqmzxn\")")
            .build();

    public static Query OP5 = new Select.QueryBuilder("distinct mostro")
            .addTable("incontro")
            .addTable("missione")
            .addTable("clima_regione")
            .addTable("status_clima")
            .addWhere("incontro.missione = missione.id and missione.regione = clima_regione.regione and " +
                    "clima_regione.clima in (select clima from status_clima) and mostro not in (select distinct mostro " +
                    "from incontro, missione, clima_regione, status_clima where incontro.missione = missione.id and " +
                    "missione.regione = clima_regione.regione and clima_regione.clima not in (select clima " +
                    "from status_clima))")
            .build();

    public static Query OP6 = new Select.QueryBuilder("nome", "account")
            .addTable("cacciatore_missioni_assegnazione")
            .addWhere("count = (select count(*) from missioni_assegnazione)")
            .build();

    public static Query OP7 = new Select.QueryBuilder("sec_to_time(avg(time_to_sec(tempo_completamento)))")
            .addTable("missione")
            .addTable("missione_completata")
            .addTable("npc")
            .addWhere("missione.id = missione_completata.missione and npc = npc.nome and " +
                    "lv_difficolta = (select max(lv_difficolta) from missione)")
            .build();

    public static Query OP8 = new Select.QueryBuilder("set_utilizzato", "count(*) as count")
            .addTable("set_equipaggiamento")
            .addTable("missione_completata")
            .addTable("missione")
            .addTable("arma")
            .addWhere("missione_completata.missione = missione.id and tipo_missione = \"taglia\" and set_utilizzato = set_equipaggiamento.id and " +
                    "arma = arma.nome and arma.tipo = \"arco\"")
            .addGroupBy("set_utilizzato")
            .addOrderBy("count", Order.DESCENDING)
            .build();

    public static Query OP9 = new Select.QueryBuilder("tipo")
            .addTable("arma")
            .addTable("set_equipaggiamento")
            .addWhere("set_equipaggiamento.id = \"00000001\" and set_equipaggiamento.arma = arma.nome")
            .build();

    public static Query OP10 = new Select.QueryBuilder("cacciatore", "account", "set_utilizzato")
            .addTable("missione")
            .addTable("missione_completata")
            .addWhere("missione_completata.missione = missione.id and tempo_completamento = (select min(tempo_completamento) " +
                    "from missione_completata, set_equipaggiamento, arma where missione_completata.set_utilizzato = set_equipaggiamento.id and " +
                    "set_equipaggiamento.arma = arma.nome and arma.tipo = \"lancia\" and missione = \"0000000096\")")
            .build();

    public static Query OP11 = new Select.QueryBuilder("gioiello.nome", "percentuale_ottenimento")
            .addTable("missione")
            .addTable("ottenimento")
            .addTable("gioiello")
            .addWhere("gioiello.nome = ottenimento.gioiello and ottenimento.missione = missione.id and " +
                    "missione.lv_difficolta = \"2\" and gioiello.nome = \"arwzfgmzorslmhzybpmshtynmgqbju\"")
            .build();

    public static Query OP12 = new Select.QueryBuilder("arma", "armatura", "gioiello")
            .addTable("set_equipaggiamento")
            .addTable("armatura_equipaggiata")
            .addTable("gioiello_equipaggiato")
            .addWhere("set_equipaggiamento.id = \"0000000001\" and armatura_equipaggiata.set_equipaggiamento = set_equipaggiamento.id and " +
                    "gioiello_equipaggiato.set_equipaggiamento = set_equipaggiamento.id")
            .build();

    public static Query OP13 = new Insert.QueryBuilder("missione_completata")
            .addValue("account", "0000000002")
            .addValue("cacciatore", "hbkmwtprwilrxxpd")
            .addValue("data_completamento", "2021-06-20 19:05:01")
            .addValue("missione", "0000000005")
            .addValue("tempo_completamento", "00:05:11")
            .addValue("set_utilizzato", "0000000001")
            .build();

    public static Query OP14 = new Select.QueryBuilder("crafting.nome")
            .addTable("crafting")
            .addTable("richiesta")
            .addTable("ricetta")
            .addWhere("crafting.nome = ricetta.oggetto_creato and ricetta.id = richiesta.ricetta and " +
                    "richiesta.oggetto_richiesto = \"aakxvowmqakysvrjgvpo\"")
            .build();

    public static Query OP15 = new Select.QueryBuilder("regione.nome")
            .addTable("regione")
            .addTable("missione")
            .addTable("incontro")
            .addWhere("incontro.missione = missione.id and missione.regione = regione.nome and " +
                    "incontro.mostro = \"aarhiuyyunlttqjxfrmc\"")
            .build();

}
