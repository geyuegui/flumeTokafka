package simplefactory.pizza;

/**
 * @author Administrator
 * @DATE 2020/7/30 0030 22:39
 * DESCRIPT
 **/
public class CheesePizza extends Pizza{
    @Override
    public void prepare() {
        System.out.println("给奶酪披萨准备原材料");
    }
}
