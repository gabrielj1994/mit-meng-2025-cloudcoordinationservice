package org.example.model;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class Record implements Serializable {

    private static final long serialVersionUID = 1L;

    private String key;
    private String owner;
    private byte[] data;

    public Record(String key, String owner, byte[] data) {
        this.key = key;
        this.owner = owner;
        this.data = data;
    }

    private static String convertByteToHexadecimal(byte[] byteArray)
    {
        String hex = "";

        // Iterating through each byte in the array
        for (byte i : byteArray) {
            hex += String.format("%02X", i);
        }

        return hex;
    }

    @Override
    public String toString() {
        return String.format("[key=%s, owner=%s, data=\n0x%s\n]", key, owner, convertByteToHexadecimal(data));
    }
}
