#! /bin/sh
cd /opt/tsr/services
(sudo java -jar ./squiggleeclient.jar -b 2015-10-01T00:00:00.000Z -e 2015-10-01T00:59:59.999Z -m ZIPF -z 120000 -i Parameter1 -l 0 -d int -x 0 -o 0 -t Cluster1 & ) && \
(sudo java -jar ./squiggleeclient.jar -b 2015-10-01T00:00:00.000Z -e 2015-10-01T00:59:59.999Z -m ZIPF -z 120000 -i Parameter2 -l 0 -d int -x 0 -o 0 -t Cluster1 & )