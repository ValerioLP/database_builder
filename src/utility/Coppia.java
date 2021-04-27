package utility;

public class Coppia<E,F> {
    private E fst;
    private F snd;

    public Coppia(E fst, F snd){
        this.fst = fst;
        this.snd = snd;
    }

    public E getFst(){return fst;}
    public F getSnd(){return snd;}

    public String toString(){
        return "("+ fst + "," + snd + ")";
    }
}