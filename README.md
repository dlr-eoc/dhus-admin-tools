# DLR supporting tools to administrate ESA DataHub Services (DHuS)

## Description

Administering the ESA DataHub Services (DHuS) http://sentineldatahub.github.io/DataHubSystem/ is supported by scripts of the provider (https://github.com/SentinelDataHub/Scripts).

The current DHuS 2.0.0-osf release tends to miss synchronizing single products from the collaborative hubs as sources. 
To be able to detect and fill this gaps, the DLR operators wrote this supporting script package.

## Contents

The scripts are located in the  top level directory. 
The script header contains instructions on its usage. 
For convenience the usage help is listed below.

### dhus-gapfill.sh

Performs OData queries a remote DHuS instances and checks output for duplicate-IDs and 
then queries local DHuS detects missing products, 
creates or updates a named synchronizer with the first 10 product IDs, 
starts it and waits for its completion.

The script is intended to be run in a cronjob to complete any misses in the previous day.

```
USAGE:

./dhus-gapfill.sh -c=... --dhus1=https://scihub.copernicus.eu/apihub --rc1=/path/to/.wgetrc1 --dhus2=http://localhost:8080 --rc2=/path/to/.wgetrc2 [-d=2020-07-06] [-a=30] [-g=_gap_sync] [-s=100] [-p=2] [-l=30] [-m] [-k] [-w] [-r]
  --condition is the full OData query, examples for each sentinel mission:
    -c="startswith(Name,'S1') and not substringof('_RAW_',Name)"
    -c="startswith(Name,'S2') and substringof('_MSIL1C_',Name)"
    -c="startswith(Name,'S3') and (substringof('_OL_',Name) or substringof('_SL_',Name) or substringof('_SR_',Name))"
    -c="startswith(Name,'S5') and substringof('_L2_',Name)"
  -d|--queryDate YYYY-MM-DD for the gap search, searches between creationDate > DATE and aquisitionDate < DATE+1day (default is yesterday)
  -a|--maxAge DD to avoid pulling reprocessed data (default is 30 days)
  --dhus1 and --dhus2 specify the base URLs of the datahub services
  --rc1 and --rc2 specify the paths to the WGETRC files containing user=xxx and password=yyy of the DHuS service accounts
  -g|--name of the synchronizer used to fill the gaps
  -s|--batchsize NUMBER to set the query batch size (default is 100)
  -p|--pagesize NUMBER to set the transfer page size (default is 2)
  -l|--limit NUMBER to limit the amount of products to query in synchronizer (default is 30)
  -m|--metadata only without product copy (default is to copy)
  -w|--wait for completion
  -r|--remove synchronizer after completion (implicit --wait)
  -k|--keep identifier lists after completion

2020-07-07T12:48:39Z INFO dhus-gapfill.sh cleanup
```

#### Example
```
./dhus-gapfill.sh -d=2019-12-18 -c="startswith(Name,'S2') and substringof('_MSIL1C_',Name)" --dhus1 https://colhub.coperncius.eu/dhus --rc1=.colhubrc --dhus2=https://dehub.dlr.de/s2hub --rc2=.dehubrc -g=_s2_gapsync --wait
```
The ```.*rc``` files contain single lines for the user=xxx and password=yyy to query the DHuS instances.

### dhus-inventory.sh

Used internally by dhus-gapfill.sh to query the product list (catalogue view) form a DhuS.

### TODO

* refactor-out the synchnronizer part into its own script, such that it can be fed with a catalogview in csv list.
* possibly reporting duplicates automatically into an EDR issue.
* change to use curl instead of wget 1.19 (which requires .netrc formated credentials)

## Change History
2020-07-07 Added past day gap synchronization and parameters --maxAge and --limit
2019-12-19 Initial commit


## Installation

Download the script package to a local unix directory.
Currently these should be called 
The scripts require bash, wget (version 1.19 or newer), curl and a few common shell utilities.

## License

This package is released under the Apache 2.0 license (see the LICENSE.txt file).

