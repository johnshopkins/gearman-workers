<?php

namespace GearmanWorkers\Elasticsearch;

class PutWorker extends Base
{
  protected function addFunctions()
  {
    parent::addFunctions();
    $this->worker->addFunction("{$this->namespace}_elasticsearch_put", array($this, "put"));
  }

  public function put(\GearmanJob $job)
  {
    $workload = json_decode($job->workload());

    if (!in_array($workload->type, $this->types)) return;

    $this->logger->addInfo("Initiating elasticsearch PUT of post #{$workload->id}...");

    try {
      $result = $this->putOne($workload->id, $workload->type);
      if ($result) {
        $this->logger->addInfo("Finished elasticsearch PUT of post #{$workload->id}.");
      }
    } catch (\Exception $e) {
      $error = $e->getMessage();
      $this->logger->addError("Put of post {$workload->id} FAILED. Error message: {$error}");
    }
  }

  /**
   * Put all fields of study present in
   * the WordPress database into elasticsearch
   * @return array $response Responses from elasticsearch
   */

  /**
   * Put all nodes whose content type are present in
   * the array of content types passed to this function
   * into the elasticsearch engine.
   * @param  array  $data         Array of content types (keys) and IDs (values)
   * @param  string $index        Elasticsearch index to put content in
   */
  public function putAll($data, $index)
  {
    foreach ($data as $type => $ids) {

      foreach ($ids as $id) {

        try {
          $result = $this->putOne($id, $type, $index);
          if ($result) {
            $this->logger->addInfo("Put of post {$type}/{$id} complete.");
          }
        } catch (\Exception $e) {
          $error = $e->getMessage();
          $this->logger->addError("Put of post {$workload->id} FAILED. Error message: {$error}");
        }
      }
    }
  }

  public function putOne($id, $type, $index = null)
  {
    $data = $this->getter->get($id, $type);

    // data is not in the API
    if (!$data) {
      $this->logger->addInfo("Post #{$id} is not in the API; deleting...");
      return $this->deleteOne($id, $type);
    }

    $params = array(
      "index" => is_null($index) ? $this->index : $index,
      "type" => $type,
      "id" => $id,
      "body" => $this->cleaner->clean($data, $type)
    );

    return $this->elasticsearchClient->index($params);
  }

  protected function getNode($id, $type)
  {
    $getterClass = "\\ElasticPosts\\Getters\\{$type}";
    $getter = new $getterClass();

    return $getter->get($id);
  }

  /**
   * Get the post data ready for elasticsearch
   * @param  object $post Post object
   * @return object Cleaned post
   */
  protected function cleanPost($post)
  {
    $condensedClass = str_replace("_", "", $post->type);
    $cleanerClass = "\\ElasticPosts\\Cleaners\\{$condensedClass}";
    if (!class_exists($cleanerClass)) {
        $cleanerClass = "\\ElasticPosts\\Cleaners\\Base";
    }
    $cleaner = new $cleanerClass();
    return $cleaner->clean($post);
  }
}
