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

    try {
      $this->putOne($workload->id, $workload->type);
    } catch (\Exception $e) {
      $error = $e->getMessage();
      $this->logger->addError("Put of post FAILED.", array(
        "post" => $workload->id,
        "error" => $error
      ));
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
        } catch (\Exception $e) {
          $error = $e->getMessage();
          $this->logger->addError("Put of post FAILED.", array(
            "post" => $workload->id,
            "error" => $error
          ));
        }
      }
    }
  }

  public function putOne($id, $type, $index = null)
  {
    $data = $this->getter->get($id, $type);

    // data is not in the API
    if (!$data) {
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
}
