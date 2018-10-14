package me.hchome.spring.ignite.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import me.hchome.spring.ignite.annotation.EnableIgnition;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.TcpDiscoveryZookeeperIpFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Start and register an ignite instance in Spring application context
 * @since 1.8+
 *
 * @author Junjie(Cliff) Pan
 */
@SuppressWarnings("unused")
public class IgniteRegistrar implements ImportBeanDefinitionRegistrar, ApplicationContextAware {

    private final static Logger LOGGER = LoggerFactory.getLogger(IgniteRegistrar.class);
    private final static ObjectMapper MAPPER = new ObjectMapper();

    private ApplicationContext context;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }

    @Override
    public void registerBeanDefinitions(AnnotationMetadata meta, BeanDefinitionRegistry registry) {
        Assert.notNull(meta, "Annotation meta data cannot be null");
        Assert.notNull(registry, "Bean registry cannot be null.");
        Map<String, Object> attributes = meta.getAnnotationAttributes(EnableIgnition.class.getCanonicalName());
        Assert.notNull(attributes, "Configuration cannot be null.");
        registerIgniteConfig(attributes, registry);
    }

    private void registerIgniteConfig(Map<String, Object> attributes, BeanDefinitionRegistry registry) {
        final EnableIgnition.IpFinderType type = (EnableIgnition.IpFinderType) attributes.get("type");
        final String value = (String) attributes.getOrDefault("value", "");
        final String attribute = (String) attributes.getOrDefault("attributes", "{}");
        final String name = (String) attributes.getOrDefault("name", "{}");
        final boolean clientMode = (boolean) attributes.getOrDefault("clientMode", false);

        IgniteConfiguration cfg = new IgniteConfiguration();
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        switch (type) { // Use different Ip finder
            case JDBC:
                spi.setIpFinder(this.configJDBCFinder(value));
                break;
            case SHARE_FS:
                spi.setIpFinder(this.configSharedFSFinder(value));
                break;
            case ZOOKEEPER:
                spi.setIpFinder(this.configZookeeperFinder(value));
                break;
            case STATIC:
                spi.setIpFinder(this.configStaticFinder(processAddress(value)));
                break;
            case MULTI_CAST:
            default:
                spi.setIpFinder(this.configMultiCastFinder(processAddress(value)));
        }

        try { // load user
            Map<String, Object> attributeMap = MAPPER.readerFor(Map.class).readValue(attribute);
            cfg.setUserAttributes(attributeMap);
        } catch (IOException ex) {
            LOGGER.error("Failed to map json", ex);
        }
        if (StringUtils.hasText(name)) cfg.setIgniteInstanceName(name);

        cfg.setDiscoverySpi(spi);
        registry.registerBeanDefinition("$APACHE#Ignite",
                BeanDefinitionBuilder
                        .genericBeanDefinition(Ignite.class, () -> {
                            Ignition.setClientMode(clientMode);
                            return Ignition.getOrStart(cfg);
                        })
                        .setDestroyMethodName("close")
                        .getBeanDefinition());
    }


    private IPDefine[] processAddress(String value) {
        if (!StringUtils.hasText(value)) return new IPDefine[]{new IPDefine("224.0.0.1", 0)};
        if (value.contains(",")) {
            String[] addresses = value.split(",");
            return Stream.of(addresses).filter(Objects::nonNull).map(address -> {
                if (address.contains(":")) {
                    String[] components = address.split(":");
                    String ad = components[0];
                    String p = components[1];
                    int port = 0;
                    if (!StringUtils.hasText(ad)) return null;
                    if (StringUtils.hasText(p) && p.matches("\\A\\d+\\Z")) port = Integer.parseInt(p);
                    return new IPDefine(ad, port);
                } else return new IPDefine(address, 0);
            }).filter(Objects::nonNull).toArray(IPDefine[]::new);
        } else if (value.contains(":")) {
            return new IPDefine[]{new IPDefine("", 0)};
        } else {
            return new IPDefine[]{new IPDefine(value)};
        }
    }

    private TcpDiscoveryIpFinder configMultiCastFinder(IPDefine[] define) {
        if (define.length == 0) return new TcpDiscoveryMulticastIpFinder();

        IPDefine multiGroup = define[0];

        IPDefine[] statics = null;
        if (define.length > 1)
            statics = Arrays.copyOfRange(define, 1, define.length, IPDefine[].class);
        return new TcpDiscoveryMulticastIpFinder()
                .setMulticastGroup(multiGroup.address)
                .setMulticastPort(multiGroup.port)
                .setAddresses(statics == null ? null : Stream.of(statics).filter(Objects::nonNull).map(IPDefine::getActualAddress).collect(Collectors.toSet()));
    }

    private TcpDiscoveryIpFinder configStaticFinder(IPDefine[] defines) {
        if (defines.length == 0) return new TcpDiscoveryMulticastIpFinder();
        return new TcpDiscoveryVmIpFinder().setAddresses(Stream.of(defines).filter(Objects::nonNull).map(IPDefine::getActualAddress).collect(Collectors.toSet()));
    }

    private TcpDiscoveryIpFinder configZookeeperFinder(String zkConnectionString) {
        return new TcpDiscoveryZookeeperIpFinder().setZkConnectionString(zkConnectionString);
    }

    private TcpDiscoveryIpFinder configSharedFSFinder(String path) {
        return new TcpDiscoverySharedFsIpFinder().setPath(path);
    }

    private TcpDiscoveryIpFinder configJDBCFinder(String value) {
        DataSource ds;
        if (!StringUtils.hasText(value)) ds = context.getBean(DataSource.class);
        else ds = context.getBean(value, DataSource.class);
        return new TcpDiscoveryJdbcIpFinder().setDataSource(ds);
    }

    private class IPDefine {
        private String address;
        private int port;

        IPDefine(String address) {
            this(address, 0);
        }

        IPDefine(String address, int port) {
            this.address = address;
            this.port = port > 0 ? port : 0;
        }

        String getActualAddress() {
            if (port > 0)
                return String.format("%s:%d", address, port);
            else
                return address;
        }
    }

}
