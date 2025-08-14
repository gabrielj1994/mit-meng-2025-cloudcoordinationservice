package org.example.util;

import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.nio.charset.StandardCharsets;

/** @author "Bikas Katwal" 26/03/19 */
public class StringSerializer implements ZkSerializer {

  @Override
  public byte[] serialize(Object data) {
    return ((String) data).getBytes();
  }

  @Override
  public Object deserialize(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
