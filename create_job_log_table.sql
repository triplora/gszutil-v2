CREATE TABLE IF NOT EXISTS `project.dataset.bqmld_job_log` (
  jobid string,
  jobdate string,
  jobtime string,
  jobname string,
  stepname string,
  procstepname string,
  symbols string,
  user string,
  script string,
  template string
)
PARTITION BY DATE(_PARTITIONTIME)
CLUSTER BY jobid
OPTIONS(
  description='mainframe job log',
  labels=[("bqmld", "zos")]
);
