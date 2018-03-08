package uni.mlgb.storm.examples;

import java.lang.reflect.InvocationTargetException;

/**
 * @author leo
 *
 * <p>TODO Load class dynamically to check the existence.</p>
 */
public class ClassNotFoundOrLoadDebugger {
    public static void main(String[] args) {
        try {
            Class<?> cls = Class.forName("uni.mlgb.storm.examples.HelloWorld");
            cls.getDeclaredMethod("main", String[].class).invoke(null, (Object)args);
            cls = Class.forName("uni.mlgb.storm.examples.ExclamationTopology");
            cls.getDeclaredMethod("main", String[].class).invoke(null, (Object)args);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
