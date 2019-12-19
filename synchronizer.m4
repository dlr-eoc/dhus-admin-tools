define(`ifnempty', `ifdef(`$1', `ifelse($1, `', `$3', `$2')', `$3')')dnl
<entry xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
    xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" 
    xmlns="http://www.w3.org/2005/Atom">
   <id>http://localhost:8080/odata/v1/Synchronizers(_ID)</id>
   <title type="text">Synchronizer</title>
   <updated>2015-06-29T09:32:15.922Z</updated>
   <category term="DHuS.Synchronizer" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme"/>
ifnempty(`_TARGETCOLLECTION', `   <link rel="http://schemas.microsoft.com/ado/2007/08/dataservices/related/TargetCollection"', `dnl')
ifnempty(`_TARGETCOLLECTION', `      type="application/atom+xml;type=entry" title="TargetCollection" href="_TARGETCOLLECTION" />', `dnl')
   <content type="application/xml">
      <m:properties>
ifnempty(`_SCHEDULE', `         <d:Schedule>_SCHEDULE</d:Schedule>', `dnl')
ifnempty(`_REQUEST', `         <d:Request>_REQUEST</d:Request>', `dnl')
ifnempty(`_SERVICEURL', `         <d:ServiceUrl>_SERVICEURL</d:ServiceUrl>', `dnl')
ifnempty(`_LABEL', `         <d:Label>_LABEL</d:Label>', `dnl')
ifnempty(`_SERVICELOGIN', `         <d:ServiceLogin>_SERVICELOGIN</d:ServiceLogin>', `dnl')
ifnempty(`_SERVICEPASSWORD', `         <d:ServicePassword>_SERVICEPASSWORD</d:ServicePassword>', `dnl')
ifnempty(`_REMOTEINCOMING', `         <d:RemoteIncoming>_REMOTEINCOMING</d:RemoteIncoming>', `dnl')
ifnempty(`_PAGESIZE', `         <d:PageSize>_PAGESIZE</d:PageSize>', `dnl')
ifnempty(`_LASTINGESTIONDATE', `         <d:LastIngestionDate>_LASTINGESTIONDATE</d:LastIngestionDate>', `dnl')
ifnempty(`_LASTCREATIONDATE', `         <d:LastCreationDate>_LASTCREATIONDATE</d:LastCreationDate>', `dnl')
ifnempty(`_COPYPRODUCT', `         <d:CopyProduct>_COPYPRODUCT</d:CopyProduct>', `dnl')
ifnempty(`_FILTERPARAM', `         <d:FilterParam>_FILTERPARAM</d:FilterParam>', `dnl')
ifnempty(`_SOURCECOLLECTION', `         <d:SourceCollection>_SOURCECOLLECTION</d:SourceCollection>', `dnl')
      </m:properties>
   </content>
</entry>
