package com.voltdb.formatters;

import java.util.Properties;

import org.voltdb.importer.formatter.AbstractFormatterFactory;
import org.voltdb.importer.formatter.Formatter;

public class JsonFormatterFactory extends AbstractFormatterFactory {

	@Override
	public Formatter create(String arg0, Properties arg1) {
		JsonFormatter jf = new JsonFormatter();
		return jf;
	}

}
