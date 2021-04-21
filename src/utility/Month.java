package utility;

import java.util.Random;

public enum Month
{
    JANUARY(1, 31),
    FEBRUARY(2, 28),
    MARCH(3,31),
    APRIL(4, 30),
    MAY(5, 31),
    JUNE(6, 30),
    JULY(7, 31),
    AUGUST(8, 31),
    SEPTEMBER(9, 30),
    OCTOBER(10, 31),
    NOVEMBER(11, 30),
    DECEMBER(12, 31);

    private int month;

    private int maxDay;

    Month(int month, int maxDay){
        this.month = month;
        this.maxDay = maxDay;
    }

    public int getMaxDay() { return maxDay; }

    public int getMonth() { return month; }

    public static Month randomMonth(){
        Random r = new Random();
        return values()[r.nextInt(values().length)];
    }
}