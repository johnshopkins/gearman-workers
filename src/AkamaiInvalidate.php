<?php

namespace GearmanWorkers;

use Akamai\Open\EdgeGrid\Authentication;

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

    // http client
    $this->http = $settings["http"];

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
    $urls = array_map(fn ($url) => str_replace('local.', 'www.', $url), $urls);

    $path = '/ccu/v3/invalidate/url/production';
    $body = json_encode(['objects' => $urls], JSON_UNESCAPED_SLASHES);
    $headers = ['Content-Type' => 'application/json'];

    $auth = new Authentication();
    $auth
      ->setAuth($this->api_auth->client_token, $this->api_auth->client_secret, $this->api_auth->access_token)
      ->setHttpMethod('POST')
      ->setHost($this->api_auth->host)
      ->setPath($path)
      ->setBody($body);

    $headers['Authorization'] = $auth->createAuthHeader();

    $this->http->post('https://' . rtrim($this->api_auth->host, '/') . $path, [
      'body' => $body,
      'headers' => $headers
    ]);

    // look for thrown exceptions
    if (!empty($this->http->log)) {
      $this->logger->warning('Cache purge failure; exception thrown.', [
        'context' => [
          'urls' => $urls,
          'log' => $this->http->log
        ],
        'tags' => [
          'gearman.handle' => $job->handle(),
          'jhu.package' => 'gearman-workers'
        ]
      ]);

      return false;
    }

    $response = $this->http->getBody();

    if ($response->httpStatus !== 201) {
      $this->logger->warning('Cache purge failure; exception thrown.', [
        'context' => [
          'urls' => $urls,
          'response' => $response,
        ],
        'tags' => [
          'gearman.handle' => $job->handle(),
          'jhu.package' => 'gearman-workers'
        ]
      ]);

      return false;
    }

    return true;
  }
}
