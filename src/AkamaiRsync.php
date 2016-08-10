<?php

namespace GearmanWorkers;

class AkamaiRsync
{
  /**
     * Gearman worker
     * @var object
     */
    protected $worker;

    /**
     * Monolog
     * @var object
     */
    protected $logger;

    protected $index = "jhu";

    public function __construct($settings = array(), $injection = array())
    {
        $this->worker = $settings["worker"];
        $this->logger = $settings["logger"];

        $this->addFunctions();
    }

    protected function addFunctions()
    {
        $this->worker->addFunction("sync_uploads", array($this, "syncUploads"));
        $this->worker->addFunction("invalidate_cache", array($this, "invalidateCache"));
    }

    public function syncUploads(\GearmanJob $job)
    {
        // $workload = json_decode($job->workload());
        // echo $this->getDate() . " Uploads sync triggered from {$workload->trigger}.\n";
        //
        // // get username/password from secrets file
        // $auth = Secret::get("jhu", ENV, "plugins", "wp-uploads-sync");
        // $username = $auth->username;
        // $password = $auth->password;
        //
        // // set password env variable
        // putenv("RSYNC_PASSWORD={$auth->password}");
        //
        // // set some directories
        // $uploadedTo = dirname($workload->file);
        // $uploadsPosition = strpos($uploadedTo, "uploads") + strlen("uploads");
        // $uploadsDirectory = substr($uploadedTo, 0, $uploadsPosition);
        // $monthDirectory = substr($uploadedTo, $uploadsPosition); // /yyyy/mm
        //
        // $source = ".{$monthDirectory}";
        // $destination = "366916/assets/uploads";
        //
        // // rsync files to Akamai using `apache` upload account
        // $command = "cd {$uploadsDirectory} && rsync -az --delete --relative {$source} {$username}@jhuwww.upload.akamai.com::{$username}/{$destination} 2>&1 > /dev/null";
        //
        // $run = exec($command, $output, $return);
        //
        // if ($return > 0) {
        //   // see http://wpkg.org/Rsync_exit_codes for rsync error codes
        //   echo $this->getDate() . " Failed to rsync uploads to Akamai. File: {$workload->file}. Rsync returned error code {$return}.\n";
        //   $this->logger->addCritical("Uploads could not be rsynced to Akamai. File: {$workload->file}. Rsync returned error code {$return}. in " . __FILE__ . " on line " . __LINE__);
        // } else {
        //   echo $this->getDate() . " Successfully rsynced uploads to Akamai.\n";
        // }

    }

    public function invalidateCache(\GearmanJob $job)
    {
      // $workload = json_decode($job->workload());
      // echo $this->getDate() . " Invalidating the cache of updated files.\n";
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

    protected function getDate()
    {
        return date("Y-m-d H:i:s");
    }
}
