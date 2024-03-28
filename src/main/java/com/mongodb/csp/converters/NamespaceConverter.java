package com.mongodb.csp.converters;

import com.mongodb.MongoNamespace;
import picocli.CommandLine;

public class NamespaceConverter implements CommandLine.ITypeConverter<MongoNamespace> {
    @Override
    public MongoNamespace convert(String s) throws Exception {
        return new MongoNamespace(s);
    }
}
