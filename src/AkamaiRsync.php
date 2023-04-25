<?php

namespace GearmanWorkers;

class AkamaiRsync
{
  public function __construct($settings = [])
  {
    // namespace (ensures no duplicate worker functions)
    $this->namespace = $settings['namespace'];

    // gearman worker
    $this->gearman_worker = $settings['gearman_worker'];

    // gearman logger
    $this->logger = $settings['logger'];

    // the directory where things will get rsynced to
    $this->directory = $settings['directory'];

    // akamsi rsync auth
    $this->username = $settings['rsync_auth']->username;
    $this->password = $settings['rsync_auth']->password;

    // akamai host (i.e. jhuwww.upload.akamai.com)
    $this->akamai_host = $settings['akamai_host'];

    // akamai api auth
    $this->api_auth = $settings['api_auth'];

    $this->callback = $settings["callback"] ?? null;

    $this->addFunctions();
  }

  protected function addFunctions()
  {
    $this->gearman_worker->addFunction("{$this->namespace}_upload", [$this, 'upload']);
    $this->gearman_worker->addFunction("{$this->namespace}_delete", [$this, 'delete']);
  }

  public function upload(\GearmanJob $job)
  {
    $handle = $job->handle();
    $workload = json_decode($job->workload());

    $debug = false;
    if (isset($workload->uri) && substr($workload->uri, -3, 3) === 'pdf') {
      $debug = true;
      $this->logger->info('UPLOAD STEP 4: AkamaiRsync::upload', [
        'workload' => (array) $workload,
        'tags' => [
          'gearman.handle' => $handle,
          'jhu.package' => 'gearman-workers'
        ]
      ]);
    }

    $this->hook('beforeUpload', $handle, $workload);

    // auth
    putenv("RSYNC_PASSWORD={$this->password}");

    $success = true;

    // rsync each file separately

    foreach ($workload->filenames as $index => $filename) {
      // Fixes bash command for files with single quotes
      // See: https://stackoverflow.com/a/1250279
      $sanitized = str_replace("'", '\'"\'"\'', $filename);
      $sourceFile = $workload->source . '/' . $sanitized;
      $command = "cd {$workload->homepath} && rsync -az --relative '{$sourceFile}' {$this->username}@{$this->akamai_host}::{$this->username}/{$this->directory} 2>&1 > /dev/null";

      if (!file_exists($workload->homepath . $sourceFile)) {
        $this->logger->warning('File that needs to be rsynced does not exist yet; waiting 5 seconds to retry', [
          'context' => ['file' => $sourceFile],
          'tags' => [
            'gearman.handle' => $handle,
            'jhu.package' => 'gearman-workers'
          ]
        ]);
        sleep(5);
      }

      $run = exec($command, $output, $return);

      if ($debug) {
        $this->logger->info('UPLOAD STEP 5: AkamaiRsync::upload run command', [
          'run_result' => $run,
          'command' => $command,
          'output' => $output,
          'return' => $return,
          'tags' => [
            'gearman.handle' => $handle,
            'jhu.package' => 'gearman-workers'
          ]
        ]);
      }

      if ($return) {

        $success = false;

        // fail
        $event = $this->logger->warning('Failed to rsync file to Akamai net storage.', [
          'context' => [
            'rsync_error' => $return,
            'handle' => $handle,
            'file' => "{$workload->source}/{$filename}",
            'output' => $output,
            'command' => $command
          ],
          'tags' => [
            'gearman.handle' => $handle,
            'jhu.package' => 'gearman-workers'
          ]
        ]);

        $this->hook('onUploadFail',
          $handle,
          $event,
          $workload->context ?? null,
          $workload->id ?? null
        );

      } else {

        // success
        $this->hook('onUploadSuccess',
          $handle,
          $filename,
          isset($workload->urls) ? $workload->urls[$index] : null,
          $workload->context ?? null,
          $workload->id ?? null
        );
      }
    }

    // when this function is called with doNormal, it needs a return value
    return $success;
  }

  public function delete(\GearmanJob $job)
  {
    $workload = json_decode($job->workload());
    $handle = $job->handle();

    // auth
    putenv("RSYNC_PASSWORD={$this->password}");

    $include = [];

    foreach ($workload->filenames as $filename) {
      $sanitized = addcslashes($filename, "'");
      $include[] = "--include=$'{$sanitized}'";
    }

    $command = "cd {$workload->homepath} && rsync -r --delete " . implode(" ", $include) ." '--exclude=*' {$workload->source}/ {$this->username}@{$this->akamai_host}::{$this->username}/{$this->directory}/{$workload->source} 2>&1 > /dev/null";
    $run = exec($command, $output, $return);

    if ($return) {
      $this->logger->warning('Failed to delete file in Akamai net storage.', [
        'context' => [
          'rsync_error' => $return,
          'file' => "{$workload->source}/{$filename}",
          'output' => $output,
          'command' => $command
        ],
        'tags' => [
          'gearman.handle' => $handle,
          'jhu.package' => 'gearman-workers'
        ]
      ]);
    }
  }

  /**
   * Call a function on the callback class
   * @param $name         string Name of function
   * @param ...$arguments mixed  Arguments to pass to the callback
   * @return void
   */
  protected function hook($name, ...$arguments): void
  {
    if ($this->callback && method_exists($this->callback, $name)) {
      call_user_func_array([$this->callback, $name], $arguments);
    }
  }
}
