<?php

namespace GearmanWorkers;

class AkamaiInvalidate
{
  public function __construct($settings = array())
  {
    // namespace (ensures no duplicate worker functions)
    $this->namespace = $settings["namespace"];

    // gearman worker
    $this->worker = $settings["worker"];

    // gearman logger
    $this->logger = $settings["logger"];

    // akamai api auth
    $this->api_auth = $settings["api_auth"];

    $this->addFunctions();
  }

  protected function addFunctions()
  {
    $this->worker->addFunction("{$this->namespace}_invalidate_urls", array($this, "invalidateUrls"));
  }

  /**
   * Invalidate cache on URLs in Akamai
   * Docs: https://developer.akamai.com/api/purge/ccu/uses.html
   * @param  GearmanJob $job object
   * @return null
   */
  public function invalidateUrls(\GearmanJob $job)
  {
    $workload = json_decode($job->workload());

    $this->logger->addInfo("purge cache");

    // setup edgegrid client
    $verbose = false;
    $client = new \Akamai\EdgeGrid($verbose, $this->api_auth);

    // setup request
    $client->path = "ccu/v3/invalidate/production";
    $client->method = "POST";
    $client->body = json_encode(array(
      "objects" => $workload->urls
    ), JSON_UNESCAPED_SLASHES);
    // $client->headers["Content-Length"] = strlen($client->body);
    $client->headers["Content-Type"] = "application/json";

    // run request.
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
