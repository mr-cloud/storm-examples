import org.junit.Test;
import uni.mlgb.storm.examples.ClassNotFoundOrLoadDebugger;

/**
 * @author leo
 */
public class TestClassLoad {
    @Test
    public void testClassLoad(){
        ClassNotFoundOrLoadDebugger.main(new String[]{"test"});
    }
}
