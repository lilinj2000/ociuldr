#!/bin/sh

LD_LIBRARY_PATH=/usr/lib:/disk1/app/oracle/product/10.2.0/db1/lib32

export LD_LIBRARY_PATH

ociuldr user=mpcaecidtraffic/mpcaecidtraffic query="select * from mpcaecidadm.productor order by id asc" field=0x7c record=0x0a file=productor.txt

ociuldr user=mpcaecidtraffic/mpcaecidtraffic query="select * from sys.test_oci" field=0x7c record=0x0a batch=10000 file=test_oci_%b.txt
