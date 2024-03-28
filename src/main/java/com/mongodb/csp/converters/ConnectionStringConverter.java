package com.mongodb.csp.converters;

import com.mongodb.ConnectionString;
import picocli.CommandLine;

public class ConnectionStringConverter implements CommandLine.ITypeConverter<ConnectionString> {
    @Override
    public ConnectionString convert(String s) throws Exception {
        return new ConnectionString(s);
    }
}
