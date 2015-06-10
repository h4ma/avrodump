package com.github.h4ma.avrodump;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.*;

class AvroDump {
	
	public static void main(String[] args) {
        if (args.length <= 1) {
            System.out.println("Usage: java -jar avrodump-0.1.0-all.jar <DIR> <EXT> [EXT EXT ...]");
            System.out.println("Example: java -jar avrodump-0.1.0-all.jar /some/dir avro");
            return;
        }
        Collection<File> files = FileUtils.listFiles(
                new File(args[0]),
                Arrays.copyOfRange(args, 1, args.length),
                true);
        for (File p: files) {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
            try {
                DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(p, datumReader);
                for (GenericRecord d: dataFileReader) {
                    System.out.println(d);
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
                System.out.println("  on " + p);
                return;
            }
        }
	}

}