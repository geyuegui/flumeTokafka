package simplefactory.pizza;

import java.awt.*;

/**
 * @author Administrator
 * @DATE 2020/7/30 0030 22:16
 * DESCRIPT
 **/
public abstract class Pizza {
    protected String name;

    public abstract void prepare();

    public void bake(){
        System.out.println(name+"  baking;");
    }
    public void cut(){
        System.out.println(name+"  cutting;");
    }

    public void box(){
        System.out.println(name+"  boxing:");
    }
    public void setName(String name){
        this.name=name;
    }

}
