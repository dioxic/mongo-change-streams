package com.mongodb.csp.converters;

import com.mongodb.csp.processors.Processor;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import picocli.CommandLine;

import java.lang.reflect.InvocationTargetException;
import java.util.NoSuchElementException;

public class ProcessorConverter implements CommandLine.ITypeConverter<Processor> {

    private static final Reflections reflections = new Reflections(new ConfigurationBuilder()
            .setUrls(ClasspathHelper.forPackage("com.mongodb.csp.processors"))
            .setScanners(Scanners.SubTypes, Scanners.TypesAnnotated)
            .setParallel(true));

    @Override
    public Processor convert(String s) throws Exception {
        for (Class<? extends Processor> clazz : reflections.getSubTypesOf(Processor.class)) {
            if (clazz.getSimpleName().equals(s)) {
                try {
                    return clazz.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | InvocationTargetException | NoSuchMethodException |
                         IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new NoSuchElementException(s + " is not found in the list of processors");
    }
}
