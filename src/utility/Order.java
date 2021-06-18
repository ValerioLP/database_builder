package utility;

public enum Order
{
    ASSCENDING("asc"),
    DESCENDING("desc");

    private String order;

    Order(String s) { order = s; }

    @Override
    public String toString() { return order; }
}
