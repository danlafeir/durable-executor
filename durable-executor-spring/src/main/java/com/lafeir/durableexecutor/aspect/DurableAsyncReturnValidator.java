package com.lafeir.durableexecutor.aspect;

import com.lafeir.durableexecutor.annotation.Durable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;

/**
 * Inspects every bean for {@code @Durable} methods with an asynchronous return type. Under
 * {@link AsyncReturnPolicy#REJECT} (default) it fails context startup with a clear error so the
 * silent loss of durability is caught at deploy time rather than at runtime; under
 * {@link AsyncReturnPolicy#ALLOW} it logs a one-time warning and lets the method through.
 */
public class DurableAsyncReturnValidator implements BeanPostProcessor {

    private static final Logger log = LoggerFactory.getLogger(DurableAsyncReturnValidator.class);

    private final AsyncReturnPolicy policy;

    public DurableAsyncReturnValidator(AsyncReturnPolicy policy) {
        this.policy = policy;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        ReflectionUtils.doWithMethods(targetClass, method -> check(targetClass, method));
        return bean;
    }

    private void check(Class<?> targetClass, Method method) {
        if (!method.isAnnotationPresent(Durable.class) || !AsyncReturnTypes.isAsync(method.getReturnType())) {
            return;
        }
        String where = targetClass.getName() + "." + method.getName() + "()";
        String returnType = method.getReturnType().getName();
        if (policy == AsyncReturnPolicy.REJECT) {
            throw new IllegalStateException(String.format(
                    "@Durable method %s returns the asynchronous type %s; only synchronous methods are "
                    + "supported because the durable record is closed when the method returns — before the "
                    + "async work completes — so a crash would lose it. Make the method synchronous, or set "
                    + "durable.async-return-policy=allow to opt out (durability is not guaranteed for the async work).",
                    where, returnType));
        }
        log.warn("@Durable method {} returns the asynchronous type {} and async-return-policy=allow; "
                + "durability is NOT guaranteed for the async work — the record is closed at hand-off.", where, returnType);
    }
}
