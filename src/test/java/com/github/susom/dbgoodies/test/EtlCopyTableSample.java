package com.github.susom.dbgoodies.test;

import com.github.susom.database.Config;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: Add class description
 *
 * @author garricko
 */
public class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    log.info("Hello World! " + Arrays.asList(args));
    log.info("Hello World! " + Config.from().custom(System.getenv()::get).systemProperties().get().getString("foo"));
  }
}
