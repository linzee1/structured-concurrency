package demo.basic;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * BasicParDemo 的基本测试
 *
 * <p>验证基础示例能够正常运行，不抛出异常。
 */
class BasicParDemoTest {

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testBasicParDemoRunsSuccessfully() {
        // 验证 BasicParDemo 的 main 方法能够正常执行
        // 这里不捕获输出，只验证不抛出异常
        BasicParDemo.main(new String[] {});
    }
}
