/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package samzaapps.wikipedia;

import joptsimple.OptionSet;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.util.CommandLine;
import org.apache.samza.util.Util;

import java.util.HashMap;
import java.util.Map;


/**
 * An entry point for {@link WikipediaApplication} that runs in stand alone mode using zookeeper.
 * It waits for the job to finish; The job can also be ended by killing this process.
 */
public class WikipediaZkLocalApplication {

  /**
   * Executes the application using the local application runner.
   * It takes two required command line arguments
   *  config-factory: a fully {@link org.apache.samza.config.factories.PropertiesConfigFactory} class name
   *  config-path: path to application properties
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Config config = cmdLine.loadConfig(options);
    Map<String, String> mergedConfig = new HashMap<>(config);
    mergedConfig.put("splitPart", String.valueOf(1));
    mergedConfig.put("job.name", "wikipedia"+1);
    Config newConfig = Util.rewriteConfig(new MapConfig(mergedConfig));
    WikipediaApplication app = new WikipediaApplication();
    LocalApplicationRunner runner = new LocalApplicationRunner(app, newConfig);
    runner.run();
    runner.waitForFinish();
  }
}
