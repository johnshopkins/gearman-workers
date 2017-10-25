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
    $this->worker->addFunction("{$this->namespace}_invalidate_page", array($this, "invalidatePage"));
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
    $response = $this->sendInvalidateRequest($workload->urls);

    // respond to error
    if ($response["error"]) {
      $this->logger->addWarning("Failed to purge file cache in Akamai.", array(
        "objects" => $workload->urls,
        "error" => $response["error"],
        "response" => $response
      ));
    }

    // respond to successful request
    if ($response["body"]) {

      $body = json_decode($response["body"]);

      if ($body->httpStatus == 201) {
        // $this->logger->addInfo("Successfully purged cache of objects in Akamai. Purge will be completed in an estimated {$body->estimatedSeconds} seconds.", array("objects" => $workload->urls, "response" => $response));
        return $body->purgeId;
      } else {
        $this->logger->addWarning("Failed to purge cache of objects in Akamai.", array(
          "code" => $body->httpStatus,
          "objects" => $workload->urls,
          "response" => $response
        ));
      }
    }

    return false;
  }

  public function invalidatePage(\GearmanJob $job)
  {
    $workload = json_decode($job->workload());

    // wait about 5 seconds before purging to allow for modules to purge
    sleep(5);

    $response = $this->sendInvalidateRequest($workload->urls);
  }

  protected function sendInvalidateRequest($urls)
  {
    // setup edgegrid client
    $verbose = false;
    $client = new \Akamai\EdgeGrid($verbose, $this->api_auth);

    // setup request
    $client->path = "ccu/v3/invalidate/url/production";
    $client->method = "POST";
    $client->body = json_encode(array(
      "objects" => $urls
    ), JSON_UNESCAPED_SLASHES);
    $client->headers["Content-Type"] = "application/json";

    // run request
    return $client->request();
  }
}
