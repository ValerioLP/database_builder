package db;

import query.Query;
import query.Select;

public class QueryMHW
{
    public static Query OP1 = new Select.QueryBuilder("arma.tipo", "count(*)")
            .addTable("missione_completata")
            .addTable("set_equipaggiamento")
            .addTable("arma")
            .addWhere("missione_completata.set_utilizzato = set_equipaggiamento.id and set_equipaggiamento.arma = arma.nome")
            .addGroupBy("arma.tipo")
            .build();

    public static Query OP2 = new Select.QueryBuilder()
            .build();
}
