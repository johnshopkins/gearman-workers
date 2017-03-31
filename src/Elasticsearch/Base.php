<?php

namespace GearmanWorkers\Elasticsearch;

use \Secrets\Secret;

abstract class Base
{
  public function __construct($settings = array())
  {
    // object with exposed "clean" method
    $this->cleaner = $settings["cleaner"];

    // object with exposed "get" method
    $this->getter = $settings["getter"];

    // elasticsearch index
    $this->index = $settings["index"];

    // gearman logger
    $this->logger = $settings["logger"];

    // app's namespace (hub, hubapi, jhu)
    $this->namespace = $settings["namespace"];

    // location of elasticsearch settings
		$this->settingsDirectory = $settings["settingsDirectory"];

    // types of content that can be pushed to elasticsearch
    $this->types = $settings["types"];

    // gearman worker
    $this->worker = $settings["worker"];

    $this->elasticsearchClient = $this->getElasticsearchClient();

    $this->addFunctions();
  }

  /**
   * Get the config for the elasticsearch box
   * @return array
   */
  protected function getElasticsearchClient()
  {
    $secrets = Secret::get("aws");
    $credentials = array(
      "accessKeyId" => $secrets->iam->es->accessKeyId,
      "secretAccessKey" => $secrets->iam->es->secretAccessKey
    );
    $http = new \HttpExchange\Adapters\Guzzle(new \GuzzleHttp\Client());
    $env = ENV;

    $host = $secrets->es->write->$env->host;
    return new \AWSElasticsearch\Client($http, $host, $credentials);
  }

  protected function addFunctions() {}

  protected function deleteOne($id, $type)
  {
    $params = array(
      "index" => $this->index,
      "type" => $type,
      "id" => $id
    );

    // Make sure the document exists in elasticsearch before deleting it
    if (!$this->elasticsearchClient->exists($params)) return;

    // delete
    return $this->elasticsearchClient->delete($params);
  }
}
