package simplefactory.pizza;

import java.awt.*;

/**
 * @author Administrator
 * @DATE 2020/7/30 0030 22:36
 * DESCRIPT
 **/
public class GreekPizza extends Pizza {
    @Override
    public void prepare() {
        System.out.println("给希腊披萨准备原材料");
    }
}
