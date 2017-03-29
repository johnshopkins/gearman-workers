<?php

namespace GearmanWorkers\Elasticsearch;

use Secrets\Secret;

class Director
{
	public function __construct($settings = array())
	{
		// function that retrieves all the IDs of a given content type
		// arguments: type
		$this->getAllOfType = $settings["getAllOfType"];

    // logger exchange
		$this->logger = $settings["logger"];

    // plugin namespace (hub, hubapi, jhu)
		$this->namespace = $settings["namespace"];

    // function that tests whether an item can be pushed to elasticsearch
    // arguments: id, type
		$this->saveTest = $settings["saveTest"];

    // types of content that can be pushed to elasticsearch
    $this->types = $settings["types"];

    $this->setupGearmanClient($settings["servers"]);
	}

  protected function setupGearmanClient($servers)
  {
    $this->gearmanClient = new \GearmanClient();

    foreach ($servers as $server) {
      $this->gearmanClient->addServer($server->hostname);
    }
  }

	public function saved($id, $type)
	{
    $saved = call_user_func($this->saveTest, $id);

    if ($saved) {
      $this->put($id, $type);
    } else {
      $this->remove($id, $type);
    }
	}

	public function put($id, $type)
	{
		return $this->gearmanClient->doBackground("{$this->namespace}_elasticsearch_put", json_encode(array(
			"id" => $id,
			"type" => $type
		)));
	}

	public function remove($id, $type)
	{
    return $this->gearmanClient->doBackground("{$this->namespace}_elasticsearch_delete", json_encode(array(
      "id" => $id,
			"type" => $type
		)));
	}

	public function reindex()
	{
		$data = array();

		foreach ($this->types as $type) {
			$data[$type] = call_user_func($this->getAllOfType, $type);
		}

		return $this->gearmanClient->doNormal("{$this->namespace}_elasticsearch_reindex", json_encode(array(
			"data" => $data
		)));
	}

}
