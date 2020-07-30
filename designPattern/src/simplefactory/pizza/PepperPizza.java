package simplefactory.pizza;

import java.awt.*;

/**
 * @author Administrator
 * @DATE 2020/7/30 0030 22:34
 * DESCRIPT
 **/
public class PepperPizza extends Pizza {
    @Override
    public void prepare() {
        System.out.println("给胡椒披萨准备原材料");
    }
}
