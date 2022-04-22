## Set up

### 1. Apache Yarn

#### 1.1 Modify the configuration files in Hadoop-vScaling/config-files/

| File                   | Configuration                                   | Parameter                                                    |
| ---------------------- | ----------------------------------------------- | ------------------------------------------------------------ |
| container-executor.cfg | yarn.nodemanager.linux-container-executor.group | your username of OS                                          |
| container-executor.cfg | allowed.system.users                            | your username of OS                                          |
| core-site.xml          | hadoop.tmp.dir                                  | ${your current directory}/vScaling/Hadoop-vScaling/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/ |
| hadoop-env.sh          | in the end                                      | "export ${your Java home}                                    |
| yarn-site.xml          | yarn.nodemanager.linux-container-executor.group | your username of OS                                          |

1.2 Set up

go to the Hadoop-vScaling/, run

```shell
sh setup.sh
```



2. Apache Samza

   go to the Samza-vScaling/ and run

   ```shell
   ./gradlew publishToMavenLocal
   ```

   