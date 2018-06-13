package part2;

/**
 * Created by Wenqiang on 5/28/18.
 */
public class Pair  {

    public String name;

    public String value;

    public Pair(String name, String value){
        this.name=name;
        this.value=value;
    }


    @Override
    public String toString() {
        return "<" + name + " , " +value+" > ";
    }



}
