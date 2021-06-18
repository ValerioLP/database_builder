package db;

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
}
