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
    return $this->sendInvalidateRequest($workload->urls, $job);
  }

  public function invalidatePage(\GearmanJob $job)
  {
    $workload = json_decode($job->workload());

    // wait about 5 seconds before purging to allow for modules to purge
    sleep(5);

    return $this->sendInvalidateRequest($workload->urls, $job);
  }

  protected function sendInvalidateRequest($urls, $job)
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
    
    /*
    cURL automatically sends a Expect: 100-continue header, which
    periodically causes an error from Akamai purge cache plugin:
      "title": "Expectation Failed",
      "status": 417,
      "detail": "Expect 100-continue header is not supported"
    Sending empty Expect header to "fix" this bug
    */
    $client->headers["Expect"] = "";

    $response = $client->request();

    if ($response["error"]) {

      // error

      $this->logger->addWarning("Cache purge failure", [
        'tags' => ['handle' => $job->handle()],
        "urls" => $urls,
        "error" => $response["error"],
        "response" => $response
      ]);

      return false;

    } else if ($response["body"]) {

      // no initial error, but it still could have failed, so check response code

      $body = json_decode($response["body"]);

      if ($body->httpStatus !== 201) {

        $this->logger->addWarning("Cache purge failure", [
          'tags' => ['handle' => $job->handle()],
          "urls" => $urls,
          "response" => $response,
          "code" => $body->httpStatus
        ]);

        return false;

      } else {

        return true;

      }
    } else {
      return false;
    }
  }
}
