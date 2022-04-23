run() {
                        echo "\$ ${@}"
                        "${@}"
                        res=$?
                        if [ $res != 0 ]; then
                          echo
                          echo "Failed!"
                          echo
                          exit $res
                        fi
                      }

                      findFileInDir(){
                        local file="$1";
                        local dir="${2:-./share}";
                        local count=$(find "$dir" -iname "$file"|wc -l)
                        echo "$count";
                      }

                      copyIfNotExists(){
                        local src="$1"
                        local srcName=$(basename "$src")
                        local dest="$2";
                        if [ -f "$src" ]; then
                          if [[ "$srcName" != *.jar ]] || [ $(findFileInDir "$srcName") -eq "0" ]; then
                            local destDir=$(dirname "$dest")
                            mkdir -p "$destDir"
                            cp "$src" "$dest"
                          fi
                        else
                          for childPath in "$src"/* ;
                          do
                            child=$(basename "$childPath");
                            if [ "$child" == "doc" ] || [ "$child" == "webapps" ]; then
                              mkdir -p "$dest"/"$child"
                              cp -r "$src"/"$child"/* "$dest"/"$child"
                              continue;
                            fi
                            copyIfNotExists "$src"/"$child" "$dest"/"$child"
                          done
                        fi
                      }

                      #Copy all contents as is except the lib.
                      #for libs check for existence in share directory, if not exist then only copy.
                      copy(){
                        local src="$1";
                        local dest="$2";
                        if [ -d "$src" ]; then
                          for childPath in "$src"/* ;
                          do
                            child=$(basename "$childPath");
                            if [ "$child" == "share" ]; then
                              copyIfNotExists "$src"/"$child" "$dest"/"$child"
                            else
                              if [ -d "$src"/"$child" ]; then
                                mkdir -p "$dest"/"$child"
                                cp -r "$src"/"$child"/* "$dest"/"$child"
                              else
                                cp -r "$src"/"$child" "$dest"/"$child"
                              fi
                            fi
                          done
                        fi
                      }

                      # Shellcheck SC2086
                      ROOT=$(cd "/home/samza/projects/work2/vScaling/Hadoop-vScaling/hadoop-dist/target"/../..;pwd)
                      echo
                      echo "Current directory $(pwd)"
                      echo
                      run rm -rf hadoop-3.0.0-SNAPSHOT
                      run mkdir hadoop-3.0.0-SNAPSHOT
                      run cd hadoop-3.0.0-SNAPSHOT
                      run cp "$ROOT"/LICENSE.txt .
                      run cp "$ROOT"/NOTICE.txt .
                      run cp "$ROOT"/README.txt .

                      # Copy hadoop-common first so that it have always have all dependencies.
                      # Remaining projects will copy only libraries which are not present already in 'share' directory.
                      run copy "$ROOT"/hadoop-common-project/hadoop-common/target/hadoop-common-3.0.0-SNAPSHOT .
                      run copy "$ROOT"/hadoop-common-project/hadoop-nfs/target/hadoop-nfs-3.0.0-SNAPSHOT .
                      run copy "$ROOT"/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.0.0-SNAPSHOT .
                      run copy "$ROOT"/hadoop-hdfs-project/hadoop-hdfs-nfs/target/hadoop-hdfs-nfs-3.0.0-SNAPSHOT .
                      run copy "$ROOT"/hadoop-yarn-project/target/hadoop-yarn-project-3.0.0-SNAPSHOT .
                      run copy "$ROOT"/hadoop-mapreduce-project/target/hadoop-mapreduce-3.0.0-SNAPSHOT .
                      run copy "$ROOT"/hadoop-tools/hadoop-tools-dist/target/hadoop-tools-dist-3.0.0-SNAPSHOT .

                      #copy httpfs and kms as is
                      run cp -r "$ROOT"/hadoop-hdfs-project/hadoop-hdfs-httpfs/target/hadoop-hdfs-httpfs-3.0.0-SNAPSHOT/* .
                      run cp -r "$ROOT"/hadoop-common-project/hadoop-kms/target/hadoop-kms-3.0.0-SNAPSHOT/* .

                      echo
                      echo "Hadoop dist layout available at: /home/samza/projects/work2/vScaling/Hadoop-vScaling/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT"
                      echo