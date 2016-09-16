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
    $this->worker->addFunction("{$this->namespace}_purge_cache", array($this, "purgeCache"));
  }

  public function upload(\GearmanJob $job)
  {
    $workload = json_decode($job->workload());

    // auth
    putenv("RSYNC_PASSWORD={$this->password}");

    // rsync each file separatly

    foreach ($workload->filenames as $filename) {
      $command = "cd {$workload->homepath} && rsync -az --relative {$workload->source}/{$filename} {$this->username}@{$this->akamai_host}::{$this->username}/{$this->directory} 2>&1 > /dev/null";
      $run = exec($command, $output, $return);

      if ($return) {
        $this->logger->addWarning("Failed to rsync file to Akamai. File: {$workload->source}/{$filename}. Rsync returned error `{$return}` in " . __FILE__ . " on line " . __LINE__, array("output" => $output, "command" => $command));
      } else {
        $this->logger->addInfo("Successfully rsynced {$workload->source}/{$filename} to Akamai");
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
      $include[] = "--include={$filename}";
    }

    $command = "cd {$workload->homepath} && rsync -r --delete " . implode($include, " ") ." '--exclude=*' {$workload->source}/ {$this->username}@{$this->akamai_host}::{$this->username}/{$this->directory}/{$workload->source} 2>&1 > /dev/null";
    $run = exec($command, $output, $return);

    if ($return) {
      $this->logger->addWarning("Failed to delete file in Akamai net storage. File: {$workload->source}/{$filename}. Rsync returned error `{$return}` in " . __FILE__ . " on line " . __LINE__, array("output" => $output));
    } else {
      $this->logger->addInfo("Successfully deleted {$workload->source}/{$filename} in Akamai net storage");
    }
  }

  /**
   * Purge cache on URLs in Akamai
   * Docs: https://api.ccu.akamai.com/ccu/v2/docs/#section_MakingaPurgeRequest
   * @param  GearmanJob $job object
   * @return null
   */
  public function purgeCache(\GearmanJob $job)
  {
    $workload = json_decode($job->workload());

    $this->logger->addInfo("purge cache");

    // setup edgegrid client
    $verbose = false;
    $client = new \Akamai\EdgeGrid($verbose, $this->api_auth);

    // setup request
    $client->path = "ccu/v2/queues/default";
    $client->method = "POST";
    $client->body = json_encode(array(
      "objects" => $workload->urls,
      "action" => "invalidate"
    ), JSON_UNESCAPED_SLASHES);
    $client->headers["Content-Length"] = strlen($client->body);
    $client->headers["Content-Type"] = "application/json";

    // run request.
    // see docs/AkamaiPurgeRequest.json for sample response
    $response = $client->request();

    // respond to error
    if ($response["error"]) {
      $this->logger->addWarning("Failed to purge file cache in Akamai in " . __FILE__ . " on line " . __LINE__, array("objects" => $workload->urls, "error" => $response["error"], "response" => $response));
      return;
    }

    // respond to successful request
    if ($response["body"]) {

      $body = json_decode($response["body"]);

      if ($body->httpStatus == 201) {
        $this->logger->addInfo("Successfully purged cache of objects in Akamai. Purge will be completed in an estimated {$body->estimatedSeconds} seconds.", array("objects" => $workload->urls, "response" => $response));
      } else {
        $this->logger->addWarning("Failed to purge cache of objects in Akamai. HTTP Response code {$body->httpStatus} in " . __FILE__ . " on line " . __LINE__, array("objects" => $workload->urls, "response" => $response));
      }
    }

  }
}
