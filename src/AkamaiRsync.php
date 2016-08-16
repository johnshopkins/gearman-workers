<?php

namespace GearmanWorkers;

class AkamaiRsync
{
  public function __construct($settings = array())
  {
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
    $this->worker->addFunction("upload", array($this, "upload"));
    $this->worker->addFunction("delete", array($this, "delete"));
    $this->worker->addFunction("invalidate_cache", array($this, "invalidateCache"));
  }

  public function upload(\GearmanJob $job)
  {
    $workload = json_decode($job->workload());

    // auth
    putenv("RSYNC_PASSWORD={$password}");

    // rsync each file separatly

    foreach ($workload->filenames as $filename) {
      $command = "cd {$workload->homepath} && rsync -az --relative {$workload->source}/{$filename} {$this->username}@{$this->akamai_host}::{$this->username}/{$this->directory} 2>&1 > /dev/null";
      $run = exec($command, $output, $return);

      if ($run > 0) {
        $this->logger->addCritical("Failed to rsync file to Akamai. File: {$workload->source}/{$filename}. Rsync returned error code {$return} in " . __FILE__ . " on line " . __LINE__);
      } else {
        $this->logger->addInfo("Successfully rsynced {$workload->source}/{$filename} to Akamai");
      }
    }

  }

  // rsync -r --delete --include=oriole-bird.jpg '--exclude=*' assets/uploads/2016/08/. apache@jhuwww.upload.akamai.com::apache/366916/testing/assets/uploads/2016/08
  public function delete(\GearmanJob $job)
  {
    $workload = json_decode($job->workload());

    // auth
    putenv("RSYNC_PASSWORD={$password}");

    // delete each file separatly

    foreach ($workload->filenames as $filename) {
      $command = "cd {$workload->homepath} && rsync -r --delete --include={$filename} {$workload->source}/ {$this->username}@{$this->akamai_host}::{$this->username}/{$this->directory} 2>&1 > /dev/null";
      $run = exec($command, $output, $return);

      if ($run > 0) {
        $this->logger->addCritical("Failed to delete in Akamai net storage. File: {$workload->source}/{$filename}. Rsync returned error code {$return} in " . __FILE__ . " on line " . __LINE__);
      } else {
        $this->logger->addInfo("Successfully deleted {$workload->source}/{$filename} in Akamai net storage");
      }
    }
  }

  public function invalidateCache(\GearmanJob $job)
  {
    // $workload = json_decode($job->workload());
    //
    // $urls = $this->getAttachmentUrls($workload->id);
    //
    // $auth = Secret::get("akamai", "rsync");
    // $verbose = false;
    // $client = new \Akamai\EdgeGrid($verbose, $auth);
    //
    // // setup request
    // $client->path = "ccu/v2/queues/default";
    // $client->method = "POST";
    // $client->body = json_encode(array(
    //   "objects" => $urls,
    //   "action" => "invalidate"
    // ), JSON_UNESCAPED_SLASHES);
    // $client->headers["Content-Length"] = strlen($client->body);
    // $client->headers["Content-Type"] = "application/json";
    //
    // // run request
    // $response = $client->request();
    //
    // // respond to error
    // if ($response["error"]) {
    //   echo $this->getDate() . " An error occured whhile invalidating the cache of updated files:\n";
    //   print_r($response["error"]);
    //   return;
    // }
    //
    // // respond to successful request
    // if ($response["body"]) {
    //
    //   $responseBody = json_decode($response["body"]);
    //
    //   if ($responseBody->httpStatus == 201) {
    //     echo $this->getDate() . " Cache of updated files successfully invalidated.\n";
    //     echo $this->getDate() . " Invalidation will take an estimated {$responseBody->estimatedSeconds} seconds.\n";
    //   } else {
    //     echo $this->getDate() . " Cache of updated files NOT invalidated.\n";
    //     print_r($responseBody);
    //   }
    //
    //   return;
    // }
    //
    // // respond in case neither error nor success
    // echo $this->getDate() . " No response or error from Akamai when trying to invalidate cache of attachment #{$workload->id}.\n";
    // $this->logger->addCritical("No response or error from Akamai when trying to invalidate cache of attachment #{$workload->id} in " . __FILE__ . " on line " . __LINE__, $response);

  }

  /**
   * Get URLs of all files related to a certain
   * attachment. Includes the original file and
   * any generated thumbnails.
   * @param integer $id Attachment IF
   */
  protected function getAttachmentUrls($id)
  {
    // get crop size names
    $crops = get_intermediate_image_sizes($id);

    // get URLs of image crops
    $urls = array_map(function ($size) use ($id) {
      $src = wp_get_attachment_image_src($id, $size);
      return $src[0];
    }, $crops);

    // add original file
    $urls[] = wp_get_attachment_url($id);

    // get rid of empty elements (files like PDF will not have thumbnail urls)
    return array_values(array_filter($urls));
  }
}
