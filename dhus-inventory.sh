#!/bin/bash
export TZ=UTC

function usage {
  >&2 echo "USAGE:"
  >&2 echo "$0 -c|--condition=... [-s|--select=Id,Name] [-b|--batchsize=100] [-l|--limit=50] \\"
  >&2 echo "     [-u|--dhusUrl=https://scihub.copernicus.eu/apihub] [-w|--wgetrc=/path/to/.wgetrc] [-q|--quiet]"
  >&2 echo "  --condition is the full OData query, examples for each sentinel mission:"
  >&2 echo "    -c=\"startswith(Name,'S1') and not substringof('_RAW_',Name)\""
  >&2 echo "    -c=\"startswith(Name,'S2') and substringof('_MSIL1C_',Name)\""
  >&2 echo "    -c=\"startswith(Name,'S3') and (substringof('_OL_',Name) or substringof('_SL_',Name) or substringof('_SR_',Name))\""
  >&2 echo "    -c=\"startswith(Name,'S5') and substringof('_L2_',Name) and ContentDate/Start ge datetime'2019-12-02T00:00:00.000' and ContentDate/Start le datetime'2019-12-02T23:59:59.999'\""
  >&2 echo "  --select fields to retrieve and return in the CSV list, default is:"  
  >&2 echo "    Id,Name,ContentLength,IngestionDate,ContentDate,Checksum,Online" 
  >&2 echo "  --dhusUrl of the data hub service (default is https://scihub.copernicus.eu/apihub)"
  >&2 echo "  --wgetrc /path/to/.wgetrc (file with user=xxx and password=yyy of the DHuS service account)"
  >&2 echo "  --batchsize for iterating over the result (default=100)"
  >&2 echo "  --limit the amount of products to be retrieved (default=50, max=500)"
  >&2 echo "  --quiet avoids progress output to stderr" 
  >&2 echo "Result is ordered by CreationDate to ensure linear sequence"
  >&2 echo "Output is sent to stdout in the CSV format with header."
  >&2 echo ""
  exit 1;
}

# defaults
condition=''
select="Id,Name,ContentLength,IngestionDate,ContentDate,Checksum,Online"
dhusUrl="https://scihub.copernicus.eu/apihub"
queryDate="$(date +%Y-%m-%d --date='1 day ago')"
batchsize=100
limit=40000
quiet=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    -c|--condition) condition="$2"; shift 2;;
    -s|--select)    select="$2"; shift 2;;
    -u|--dhusUrl)   dhusUrl="$2"; shift 2;;
    -w|--wgetrc)    export WGETRC="$2"; shift 2;;
    -b|--batchsize) batchsize="$2"; shift 2;;
    -l|--limit)     limit="$2"; shift 2;;
    -q|--quiet)     quiet=true; shift 1;;

    -c=*|--condition=*) condition="${1#*=}"; shift 1;;
    -s=*|--select=*)    select="${1#*=}"; shift 1;;
    -u=*|--dhusUrl=*)   dhusUrl="${1#*=}"; shift 1;;
    -w=*|--wgetrc=*)    export WGETRC="${1#*=}"; shift 1;;
    -b=*|--batchsize=*) batchsize="${1#*=}"; shift 1;;
    -l=*|--limit=*)     limit="${1#*=}"; shift 1;;

    *) echo "ERROR: unknown option '$1'"; usage; exit;;
  esac
done

if [ "$condition" == "" ]; then
  >&2 echo "ERROR: no condition defined!"
  echo ""
  usage
  exit 1
fi
$quiet || >&2 echo "Listing with condition:"
$quiet || >&2 echo "$condition"

# query for new data
omitHeader=0
skip=""
odataQuery="\$top=$batchsize&\$select=$select&\$format=text/csv&\$orderby=CreationDate asc&\$filter=$condition"
pos=0
while [ $pos -le $limit ]
do
  lines=( $(/usr/bin/wget -q -O - "$dhusUrl/odata/v1/Products/?$skip$odataQuery" ) )
  status=${PIPESTATUS[0]}
  if [ $status != 0 ]; then
    >2& echo "query failed with status=$status"  
    if [ ${#lines[@]} > 0 ]; then
      >2& printf '%s\n' "${lines[@]}"
    fi
    break
  fi 
  printf -- '%s\n' "${lines[@]: $omitHeader}"

  # advance retrieval position
  pos=$(($pos + ${#lines[@]} - 1))
  $quiet || >&2 echo -n "... $pos "

  # check for incomplete batch
  if [ ${#lines[@]} -le $batchsize ]; then
    break;
  fi 
  skip="\$skip=${pos}&"
  omitHeader=1
done

## finish printing progress line
$quiet || >&2 echo ""
