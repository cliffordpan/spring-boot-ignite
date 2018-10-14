package me.hchome.spring.ignite.annotation;

import me.hchome.spring.ignite.config.IgniteRegistrar;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Enable Apache Ignite
 *
 * @author Junjie(Cliff) Pan
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({IgniteRegistrar.class})
@SuppressWarnings("unused")
@Documented
public @interface EnableIgnition {

    /**
     * Ignite discovery ip finder type.
     * Check ignite documentation for details.
     * */
    IpFinderType type() default IpFinderType.MULTI_CAST;

    /**
     * Ignite instance name
     * */
    String name() default "";

    /**
     * Set ignite start as client mode or server. Default is server mode.
     * */
    boolean clientMode() default false;

    /**
     * Set user attributes use a json format.
     * */
    String attributes() default "{}";

    /**
     * Set the address value for multiple cast, static ip, or set connection string for zookeeper,
     * or set the share file system path, or set the data source name reference in spring context.
     * */
    String value() default "";

    enum IpFinderType {
        ZOOKEEPER, MULTI_CAST, STATIC, SHARE_FS, JDBC
    }

}
