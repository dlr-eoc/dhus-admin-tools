#!/bin/bash
export TZ=UTC

function usage {
  >&2 echo "This script will compare the products ingested for one day in two DataHubs and will "
  >&2 echo "USAGE:"
  >&2 echo "$0 -c|--condition=... --dhus1=$dhus1 --rc1=/path/to/.wgetrc1 --dhus2=$dhus2 --rc2=/path/to/.wgetrc2 [-d|date=$queryDate] [-g|--name=gap-synchronizer-name] [-s|--batchSize=100] [-m|--metadata] [-k|--keeplists] [-w|--wait] [-r|--remove]"
  >&2 echo "  --condition is the full OData query, examples for each sentinel mission:"
  >&2 echo "    -c=\"startswith(Name,'S1') and not substringof('_RAW_',Name)\""
  >&2 echo "    -c=\"startswith(Name,'S2') and substringof('_MSIL1C_',Name)\""
  >&2 echo "    -c=\"startswith(Name,'S3') and (substringof('_OL_',Name) or substringof('_SL_',Name) or substringof('_SR_',Name))\""
  >&2 echo "    -c=\"startswith(Name,'S5') and substringof('_L2_',Name)\""
  >&2 echo "  --queryDate YYYY-MM-DD for the gap search, searches between creationDate > DATE and aquisitionDate < DATE+1day (default is yesterday)"
  >&2 echo "  --dhus1 and --dhus2 specify the base URLs of the datahub services"
  >&2 echo "  --rc1 and --rc2 specify the paths to the WGETRC files with user=xxx and password=yyy of the DHuS service accounts"
  >&2 echo "  -g|--name of the synchronizer used to fill the gaps"
  >&2 echo "  --batchsize NUMBER to set the query batch size" 
  >&2 echo "  --metadata only without product copy (default is to copy)   
  >&2 echo "  --wait for completion"
  >&2 echo "  --remove synchronizer after completion (implicit --wait)"
  >&2 echo "  --keep identifier lists after completion" 
  >&2 echo ""
  exit 1;
}

# defaults
condition=''
select="Id,Name,ContentLength,IngestionDate,ContentDate,Checksum,Online"
dhus1="https://scihub.copernicus.eu/apihub"
rc1=.wgetrc1
dhus2="http://localhost:8080"
rc2=.wgetrc2
queryDate="$(date +%Y-%m-%d --date='1 day ago')"
dayonly=false
batchsize=100
wait=false;
remove=false;
copyproduct=true;
gapsync="_gap_sync"
keeplists=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    -c|--condition) condition="$2"; shift 2;;
    -d|--queryDate) queryDate="$2"; shift 2;;
    -g|--name)      gapsync="$2"; shift 2;;
    --dhus1)        dhus1="$2"; shift 2;;
    --dhus2)        dhus2="$2"; shift 2;;
    --rc1)          rc1="$2"; shift 2;;
    --rc2)          rc2="$2"; shift 2;;
    --batchSize)    batchsize="$2"; shift 2;;

    -c=*|--condition=*) condition="${1#*=}"; shift 1;;
    -d=*|--queryDate=*) queryDate="${1#*=}"; dayonly=true; shift 1;;
    -g=*|--name=*)  gapsync="${1#*=}"; shift 1;;
    --dhus1=*)      dhus1="${1#*=}"; shift 1;;
    --dhus2=*)      dhus2="${1#*=}"; shift 1;;
    --rc1=*)        rc1="${1#*=}"; shift 1;;
    --rc2=*)        rc2="${1#*=}"; shift 1;;
    --batchSize=*)  batchsize="${1#*=}"; shift 1;;

    -m|--metadata)  copyproduct=false; shift 1;;
    -w|--wait)      wait=true; shift 1;;
    -r|--remove)    remove=true; wait=true; shift 1;;
    -k|--keeplists) keep=true; shift 1;;

    *) echo "ERROR: unknown option '$1'"; usage; exit;;
  esac
done

function log() {
  echo "$(date +%Y-%m-%dT%H:%M:%SZ) $(if [[ $# -ne 2 ]]; then echo INFO; else echo $1; fi) ${BASH_SOURCE[1]##*/} ${2:-$1}"
}
export -f log

function logerr() { 
  cat <<< "$(date +%Y-%m-%dT%H:%M:%SZ) ERROR ${BASH_SOURCE[1]##*/} $@" >&2
}

# temporary files
tmpdir=/tmp
scriptname=${0##*/}
list1=$tmpdir/${scriptname}_list1_$$
list2=$tmpdir/${scriptname}_list2_$$
ids1=$tmpdir/${scriptname}_ids1_$$
ids2=$tmpdir/${scriptname}_ids2_$$
missing=$tmpdir/${scriptname}_missing_$$
syncfile=$tmpdir/${scriptname}_synchronizer_$$
# cleanup after exit
trap "log "cleanup"; if [[ "$remove" == "false" ]]; then rm -f $list1 $list2; fi; rm -f $ids1 $ids2 $missing $syncfile | true" EXIT
  
if [ "$condition" == "" ]; then
  logerr "no condition defined!"
  echo ""
  usage
  exit 1
fi
# only data synchronized at day of interest or later
condition="$condition and CreationDate ge datetime'${queryDate}T00:00:00.000'"
# avoid overlap to data being synchronized at the moment (4 hours latency)
##condition="$condition and CreationDate le datetime'$(date '+%Y-%m-%dT%H:%M:%S' --date='4 hours ago')'" 
# only data for that day or earlier
condition="$condition and ContentDate/Start le datetime'${queryDate}T23:59:59.999'"
if [[ $dayonly == "true" ]]; then
  # only data for that specific day
  condition="$condition and ContentDate/Start ge datetime'${queryDate}T00:00:00.000'"
  # otherwise also allows synchronizing gaps at earlier dates of data arriving later
fi
log "Comparing with condition:"
log "  $condition"

# retrieve daily inventory from dhus1
log "... reading daily inventory from $dhus1"
SECONDS=0
./dhus-inventory.sh -c="$condition" -b=$batchsize -u="$dhus1" -w="$rc1" --select="Id,Name,ContentLength,CreationDate" > $list1
log "... daily inventory from $dhus1 has $(cat $list1 | wc -l) products in $SECONDS seconds"

# retrieve daily inventory from dhus2
log "... reading daily inventory from $dhus2"
SECONDS=0
./dhus-inventory.sh -c="$condition" -b=$batchsize -u="$dhus2" -w="$rc2" --select="Id,Name,ContentLength,CreationDate" > $list2   
log "... daily inventory from $dhus2 has $(cat $list2 | wc -l) products in $SECONDS seconds"

# check for duplicates
cut -s -d, -f2 $list1 | sort -u > $ids1
duplicates=$(($(cat $list1 | wc -l) - $(cat $ids1 | wc -l)))
if [ $duplicates -ne 0 ]; then
  log WARN "$duplicates duplicates in $dhus1 on ${queryDate}"
  grep -Ff <(cut -s -d, -f2 $list1 | sort | uniq -d) $list1 | sort -t, -k2
fi
cut -s -d, -f2 $list2 | sort -u > $ids2
duplicates=$(($(cat $list2 | wc -l) - $(cat $ids2 | wc -l)))
if [ $duplicates -ne 0 ]; then
  log WARN "$duplicates duplicates in $dhus2 on ${queryDate}"
  grep -Ff <(cut -s -d, -f2 $list2 | sort | uniq -d) $list2 | sort -t, -k2
fi

# compare inventories by ID
log "Comparing $(cat $list1 | wc -l) with $(cat $list2 | wc -l) lines"
grep -Ff <(comm -23 $ids1 $ids2) $list1 > $missing

# process missing files
log "Missing $(cat $missing | wc -l) products"
head $missing

# date range of products to retrieve
firstCreationdate="$(cut -d, -f4 $missing |sort |head -1 | cut -dT -f1)T00:00:00.000"
lastCreationdate=$(cut -d, -f4 $missing |sort -r |head -1 |tr -d '\n\r')

export WGETRC=$rc2
if [[ $(cat $missing | wc -l) > 0 ]]; then
  log "synchronizing $(cat $missing | wc -l) missing products"
  filter=$(cat $missing | cut -d, -f2 | xargs -n1 -I% echo -n "or Name eq '%' " | sed -e 's/^or /(/'; echo ')' )  
  user=$(grep user $rc1 | cut -d= -f2)
  pass=$(grep pass $rc1 | cut -d= -f2)
  params=(-D_SCHEDULE='0 */1 * * * ?' -D_SERVICEURL=$dhus1/odata/v1 -D_LABEL=$gapsync \
          -D_SERVICELOGIN=$user -D_SERVICEPASSWORD=$pass -D_PAGESIZE=2  -D_REQUEST=start \
          -D_LASTCREATIONDATE=$firstCreationdate -D_COPYPRODUCT=$copyproduct -D_FILTERPARAM="$filter")
  # check if synchronizer exists
  synchronizer=$(wget -q -O - "$dhus2/odata/v1/Synchronizers/?\$format=text/csv&\$select=Id,Label,LastCreationDate,Status" |grep $gapsync | tr -d '\r\n')
  if [[ "$synchronizer" == "" ]]; then
    log "create gap synchronizer $gapsync with $filter"
    m4 -D_ID="0L" "${params[@]}" synchronizer.m4 > $syncfile
    wget -q -O - --method=POST --body-file=$syncfile \
        --header "Content-Type:application/atom+xml" --header "Accept:application/atom+xml" \
        "$dhus2/odata/v1/Synchronizers" | xmllint --format -
  else
    log "update gap synchronizer $gapsync with $filter"
    id=${synchronizer%%,*}
    m4 -D_ID="${id}L" "${params[@]}" synchronizer.m4 > $syncfile
    wget -q -O - --method=PUT --body-file=$syncfile \
        --header "Content-Type:application/atom+xml" \
        "$dhus2/odata/v1/Synchronizers(${id})"
  fi
fi

# wait for completion
if [[ ($wait == "true") && $(cat $missing | wc -l) > 0 ]]; then
  synchronizer=$(wget -q -O - "$dhus2/odata/v1/Synchronizers/?\$format=text/csv&\$select=Id,Label,LastCreationDate,Status" |grep $gapsync | tr -d '\r\n')
  if [[ "$synchronizer" != "" ]]; then
    while [[ ("${synchronizer##*,}" != "STOPPED") && ("${synchronizer##*,}" != "ERROR") && $(echo $synchronizer | cut -d, -f3) < $lastCreationdate ]]; 
    do
      log "... waiting for $synchronizer to complete"  
      sleep 60;
      synchronizer=$(wget -q -O - "$dhus2/odata/v1/Synchronizers/?\$format=text/csv&\$select=Id,Label,LastCreationDate,Status" |grep $gapsync | tr -d '\r\n')
    done
    log "... completed $synchronizer"  
    id=${synchronizer%%,*}
    if [[ "$remove" == "true" ]]; then
      log "... deleting $synchronizer"
      wget -q -O - --method=DELETE "$dhus2/odata/v1/Synchronizers(${id})"
    else
      log "stop $gapsync"
      params=(-D_REQUEST=stop)
      m4 -D_ID="${id}L" "${params[@]}" synchronizer.m4 > $syncfile
      wget -q -O - --method=PUT --body-file=$syncfile \
          --header "Content-Type:application/atom+xml" \
          "$dhus2/odata/v1/Synchronizers(${id})"
    fi
  else
    logerr "cannot wait for unknown synchronizer $gapsync"
  fi
fi

log "Done."
