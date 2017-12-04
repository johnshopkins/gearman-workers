<?php

namespace GearmanWorkers\Elasticsearch;

class DeleteWorker extends Base
{
  protected function addFunctions()
  {
    parent::addFunctions();
    $this->worker->addFunction("{$this->namespace}_elasticsearch_delete", array($this, "delete"));
  }

  public function delete(\GearmanJob $job)
  {
    $workload = json_decode($job->workload());

    try {
      $this->deleteOne($workload->id, $workload->type);
    } catch (\Exception $e) {
      $error = $e->getMessage();
      $this->logger->addError("Delete of post FAILED.", array(
        "post" => $workload->id,
        "error" => $error
      ));
    }
  }
}
