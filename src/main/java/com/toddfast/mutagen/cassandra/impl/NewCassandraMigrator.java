/**
 * Copyright 2010-2014 Restlet S.A.S. All rights reserved.
 * 
 * Restlet and APISpark are registered trademarks of Restlet S.A.S.
 */

package com.toddfast.mutagen.cassandra.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;

/**
 * Base classe for cassandra migration tool.
 * 
 * @author thboileau
 * 
 */
public abstract class NewCassandraMigrator extends AbstractCassandraMutation {

    private static Logger LOGGER = LoggerFactory.getLogger(NewCassandraMigrator.class);

    /**
     * Empty constructor.
     */
    public NewCassandraMigrator() {
        this(null);
    }

    /**
     * constructor with session.
     * 
     * @param session
     *            - session.
     */
    public NewCassandraMigrator(Session session) {
        super(session);
    }


    /**
     * Override to add migration code.
     */
    // protected abstract void migrate();

    // Called only by the mutagen framework
    @Override
    protected abstract void performMutation(Context context);


    @Override
    // return class name (with package hierarchy) and replace semicolons by "/" for correct version parsing
    // if using semicolons package names ending with integers will be confused with mutation state
    protected String getResourceName() {
        String name = getClass().getName();
        return name.substring(name.lastIndexOf(".") + 1) + ".java";
    }

    @Override
    public String getChecksum() {
        LOGGER.trace("Entering getChecksum()");
        try {
            String checksum = toHex(getDigestFromArray(getClassContents(this.getClass())));

            LOGGER.trace("Leaving getChecksum() : {}", checksum);
            return checksum;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("unable to get checksum");
        }

    }

    public static final byte[] getClassContents(Class<?> myClass) throws IOException {

        LOGGER.trace("Entering getClassContents(class={})", myClass);

        String path = myClass.getName().replace('.', '/');
        String fileName = new StringBuffer(path).append(".class").toString();
        InputStream is = myClass.getClassLoader().getResourceAsStream(fileName);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int datum = is.read();
        while (datum != -1) {
            buffer.write(datum);
            datum = is.read();
        }

        is.close();
        LOGGER.trace("Leaving getClassContents()");

        return buffer.toByteArray();
    }

    public byte[] getDigestFromArray(byte[] array) {
        byte[] output = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(array);
            output = md.digest();
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return output;
    }
}
