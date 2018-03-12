package uni.mlgb.storm.examples;

import org.apache.storm.Config;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.utils.DRPCClient;
import org.apache.thrift.TException;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

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
//            cls = Class.forName("uni.mlgb.storm.examples.ExclamationTopology");
//            cls.getDeclaredMethod("main", String[].class).invoke(null, (Object)args);
            cls = Class.forName("org.apache.storm.utils.DRPCClient");
            Config conf = new Config();
            conf.setNumWorkers(1);
            try (DRPCClient drpc = (DRPCClient) cls.getDeclaredMethod("getConfiguredClient", new HashMap<String, Object>().getClass()).invoke(null, (Object)conf)) {
                String[] urlsToTry = new String[]{ "foo.com/blog/1", "engineering.twitter.com/blog/5", "notaurl.com" };
                for (String url : urlsToTry) {
                    System.out.println("Reach of " + url + ": " + drpc.execute("reach", url));
                }
            } catch (DRPCExecutionException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            } catch (TException e) {
                e.printStackTrace();
            }

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
