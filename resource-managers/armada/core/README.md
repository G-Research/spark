Armada Cluster Manager for Spark
---

Build
---
From the spark repository root: 
```bash
./build/sbt package -Pkubernetes -Parmada
```
You may have to adjust `JAVA_HOME` to suit your environment. `-Parmada` tells
sbt to enable the armada project.

Once at the sbt console:
```
sbt:spark-parent> project armada
```
Switches to the aramda project.

Now with feeling:
```
sbt:spark-armada> package
```
Should build what's available underneath `spark/resource-managers/armada`.

Test
---
```
sbt:spark-armada> test
```
Runs all tests!
