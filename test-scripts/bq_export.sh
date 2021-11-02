clear
export PROJECT_ID=$(gcloud config get-value project)

export LOG_WRAP_SPOOL=false

export QUERY=$PWD/bq_export.sql
export QUERY_LRECL=5000
export QUERY_BLKSIZE=5000

export OUTFILE=$PWD/bq_export.outfile
export OUTFILE_LRECL=80
export OUTFILE_BLKSIZE=512

export COPYBOOK=$PWD/bq_export.cp

export SBT_OPTS=" -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"
rm $OUTFILE

cd ..
sbt "runMain com.google.cloud.bqsh.Bqsh" << EOF
bq export \
  --project_id=$PROJECT_ID \
  --location="US" \
  --run_mode=storage_api \
  --outDD=OUTFILE
EOF
