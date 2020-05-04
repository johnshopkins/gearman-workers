<?php

namespace GearmanWorkers;

class AkamaiRsync
{
  public function __construct($settings = array())
  {
    // namespace (ensures no duplicate worker functions)
    $this->namespace = $settings["namespace"];

    // gearman worker
    $this->worker = $settings["worker"];

    // gearman logger
    $this->logger = $settings["logger"];

    // the directory where things will get rsynced to
    $this->directory = $settings["directory"];

    // akamsi rsync auth
    $this->username = $settings["rsync_auth"]->username;
    $this->password = $settings["rsync_auth"]->password;

    // akamai host (i.e. jhuwww.upload.akamai.com)
    $this->akamai_host = $settings["akamai_host"];

    // akamai api auth
    $this->api_auth = $settings["api_auth"];


    $this->addFunctions();
  }

  protected function addFunctions()
  {
    $this->worker->addFunction("{$this->namespace}_upload", array($this, "upload"));
    $this->worker->addFunction("{$this->namespace}_delete", array($this, "delete"));
  }

  public function upload(\GearmanJob $job)
  {
    $handle = $job->handle();
    $workload = json_decode($job->workload());

    // auth
    putenv("RSYNC_PASSWORD={$this->password}");

    // rsync each file separatly

    foreach ($workload->filenames as $filename) {
      $sanitized = addcslashes($filename, "'");
      $command = "cd {$workload->homepath} && rsync -az --relative '{$workload->source}/{$sanitized}' {$this->username}@{$this->akamai_host}::{$this->username}/{$this->directory} 2>&1 > /dev/null";
      $run = exec($command, $output, $return);

      if ($return) {
        $event = $this->logger->addWarning("Failed to rsync file to Akamai net storage.", array(
          "rsync_error" => $return,
          "handle" => $handle,
          "file" => "{$workload->source}/{$filename}",
          "output" => $output,
          "command" => $command
        ));
      }
    }

  }

  public function delete(\GearmanJob $job)
  {
    $workload = json_decode($job->workload());

    // auth
    putenv("RSYNC_PASSWORD={$this->password}");

    $include = array();

    foreach ($workload->filenames as $filename) {
      $sanitized = addcslashes($filename, "'");
      $include[] = "--include=$'{$sanitized}'";
    }

    $command = "cd {$workload->homepath} && rsync -r --delete " . implode($include, " ") ." '--exclude=*' {$workload->source}/ {$this->username}@{$this->akamai_host}::{$this->username}/{$this->directory}/{$workload->source} 2>&1 > /dev/null";
    $run = exec($command, $output, $return);

    if ($return) {
      $this->logger->addWarning("Failed to delete file in Akamai net storage.", array(
        "rsync_error" => $return,
        "file" => "{$workload->source}/{$filename}",
        "output" => $output,
        "command" => $command
      ));
    }
  }

}
