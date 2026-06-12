package com.lafeir.durableexecutor.aspect;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

/**
 * Detects whether a method return type represents asynchronous work that outlives the method call.
 * Reactive types are matched by name so reactor / reactive-streams / RxJava need not be on the
 * classpath.
 */
final class AsyncReturnTypes {

    private static final Set<String> REACTIVE_TYPE_NAMES = Set.of(
            "org.reactivestreams.Publisher",
            "reactor.core.publisher.Mono",
            "reactor.core.publisher.Flux",
            "io.reactivex.rxjava3.core.Flowable",
            "io.reactivex.rxjava3.core.Observable",
            "io.reactivex.rxjava3.core.Single",
            "io.reactivex.rxjava3.core.Maybe",
            "kotlinx.coroutines.flow.Flow"
    );

    private AsyncReturnTypes() {}

    static boolean isAsync(Class<?> returnType) {
        if (returnType == null) {
            return false;
        }
        if (Future.class.isAssignableFrom(returnType) || CompletionStage.class.isAssignableFrom(returnType)) {
            return true;
        }
        for (String name : typeAndSupertypeNames(returnType)) {
            if (REACTIVE_TYPE_NAMES.contains(name)) {
                return true;
            }
        }
        return false;
    }

    private static Set<String> typeAndSupertypeNames(Class<?> type) {
        Set<String> names = new HashSet<>();
        Deque<Class<?>> toVisit = new ArrayDeque<>();
        toVisit.push(type);
        while (!toVisit.isEmpty()) {
            Class<?> current = toVisit.pop();
            if (current == Object.class || !names.add(current.getName())) {
                continue;
            }
            Class<?> superclass = current.getSuperclass();
            if (superclass != null) { // null for interfaces, primitives, and void
                toVisit.push(superclass);
            }
            for (Class<?> iface : current.getInterfaces()) {
                toVisit.push(iface);
            }
        }
        return names;
    }
}
