# Vertical Scaling Framework

The vertical scaling framework is designed for realtime stream processing on both computation and memory resources.

## Set up

#### Apache Yarn

1. Modify the configuration files in Hadoop-vScaling/config-files/

| File                   | Configuration                                   | Parameter                                                    |
| ---------------------- | ----------------------------------------------- | ------------------------------------------------------------ |
| container-executor.cfg | yarn.nodemanager.linux-container-executor.group | your username of OS                                          |
| container-executor.cfg | allowed.system.users                            | your username of OS                                          |
| core-site.xml          | hadoop.tmp.dir                                  | ${your current directory}/vScaling/Hadoop-vScaling/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/ |
| hadoop-env.sh          | in the end                                      | "export ${your Java home}                                    |
| yarn-site.xml          | yarn.nodemanager.linux-container-executor.group | your username of OS                                          |

2. Install

go to the Hadoop-vScaling/, run

```shell
sh setup.sh ${your username of OS}
#no worry about the hadoop-dist compile error
```



#### Apache Samza

go to the Samza-vScaling/ and run

```shell
./gradlew publishToMavenLocal
```



## Evaluation

#### Stock Application

Due to non-disclosure agreements, we are currently unable to publish the stock data used for reproduction.



#### Nexmark Application

To get figures, make sure you have installed **Python** (not Python 3), and **matplotlib** libraries

1. For comparion with Elasticutor (Section 5.1), run

   ```shell
   cd samza-testbed/nexmark_scripts
   #similarily, for q2, q3..., q11, replace 1 with 2, 3,..., 11 in the following command.
   sh run-comp.sh 1
   ```

   

2. For effectiveness experiments (Section 5.2), run

   ```shell
   cd samza-testbed/nexmark_scripts
   sh run-effect.sh
   ```



Output data will be found in tools/results/.

In the next few days, we will publish the scripts to automatically generate the figures.

