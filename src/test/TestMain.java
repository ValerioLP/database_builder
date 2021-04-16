package test;

import data.*;
import db.Attribute;
import db.Database;
import db.Table;
import db.Vincolo;
import exceptions.ForeignKeyException;

public class TestMain
{
    private static final String URL = "jdbc:mysql://127.0.0.1:3306/";

    private static final DataType CF = new VarChar(16);

    public static void main(String[] args) throws ForeignKeyException
    {
        Account account = new Account("root", "1febbraio1992");

        Table t1 = new Table.TableBuilder("PERSONA")
                .addAttribute(new Attribute.AttributeBuilder("cf", CF)
                        .addKey()
                        .addUnique()
                        .build())
                .addAttribute(new Attribute.AttributeBuilder("nome", new VarChar(20))
                        .addNotNull()
                        .build())
                .addAttribute(new Attribute.AttributeBuilder("data_nascita", new Date())
                        .addNotNull()
                        .build())
                .build();

        Table t2 = new Table.TableBuilder("MATRIMONIO")
                .addAttribute(new Attribute.AttributeBuilder("codice", new Int())
                        .addUnique()
                        .addFill()
                        .addAutoIncremental()
                        .addKey()
                        .build())
                .addAttribute(new Attribute.AttributeBuilder("cf_moglie", CF)
                        .addNotNull()
                        .build())
                .addAttribute(new Attribute.AttributeBuilder("cf_marito", CF)
                        .addNotNull()
                        .build())
                .addAttribute(new Attribute.AttributeBuilder("data", new Date())
                        .addNotNull()
                        .build())
                .addAttribute(new Attribute.AttributeBuilder("numero_invitati", new Int())
                        .addNotNull()
                        .build())
                .addVincolo(new Vincolo("cf_moglie", t1, "cf"))
                .addVincolo(new Vincolo( "cf_marito", t1, "cf"))
                .build();

        Table t3 = new Table.TableBuilder("TESTIMONI")
                .addAttribute(new Attribute.AttributeBuilder("cf_testimone", CF)
                        .addKey()
                        .build())
                .addAttribute(new Attribute.AttributeBuilder("codice_matrimonio", new Int())
                        .addKey()
                        .addFill()
                        .build())
                .addVincolo(new Vincolo("cf_testimone" , t1,"cf"))
                .addVincolo(new Vincolo( "codice_matrimonio", t2, "codice"))
                .build();

        Database db = new Database.DatabaseBuilder("matrimoni", URL, account)
                .addTable(t1)
                .addTable(t2)
                .addTable(t3)
                .build();
    }
}