package data;

/**
 * @author Luca Mattei, Valerio Mezzoprete
 */
public class Account
{
    /**
     * Campi della classe:
     */
    private String username;

    private String password;

    /**
     * costruttore della classe
     * @param username nome dell'account che si connetterà al db
     * @param password password dell'account che si connetterà al db
     */
    public Account(String username, String password)
    {
        this.username = username;
        this.password = password;
    }

    /**
     * setter
     * @param username nuovo username
     */
    public void setUsername(String username) { this.username = username; }

    /**
     * setter
     * @param password nuova password
     */
    public void setPassword(String password) { this.password = password; }

    /**
     * getter che ritorna lo username
     * @return usenrame
     */
    public String getUsername() { return username; }

    /**
     * getter che ritorna la password
     * @return password
     */
    public String getPassword() { return password; }

    @Override
    public String toString() { return username; }
}