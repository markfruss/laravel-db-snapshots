<?php

namespace Spatie\DbSnapshots;

use Carbon\Carbon;
use Illuminate\Filesystem\FilesystemAdapter as Disk;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\DB;
use Spatie\TemporaryDirectory\TemporaryDirectory;
use Symfony\Component\Process\Process;
use Spatie\DbSnapshots\Events\DeletedSnapshot;
use Spatie\DbSnapshots\Events\DeletingSnapshot;
use Spatie\DbSnapshots\Events\LoadedSnapshot;
use Spatie\DbSnapshots\Events\LoadingSnapshot;
use Spatie\DbSnapshots\Events\DownloadedSnapshot;
use Spatie\DbSnapshots\Events\DownloadingSnapshot;

class Snapshot
{
    public Disk $disk;

    public string $fileName;

    public string $name;

    public ?string $compressionExtension = null;

    private const CHUNK_SIZE = 1024*1024*8;

    public function __construct(Disk $disk, string $fileName)
    {
        $this->disk = $disk;

        $this->fileName = $fileName;

        $pathinfo = pathinfo($fileName);

        if ($pathinfo['extension'] === 'gz') {
            $this->compressionExtension = $pathinfo['extension'];
            $fileName = $pathinfo['filename'];
        }

        $this->name = pathinfo($fileName, PATHINFO_FILENAME);
    }

    public function load(string $connectionName = null)
    {
        event(new LoadingSnapshot($this));

        if ($connectionName !== null) {
            DB::setDefaultConnection($connectionName);
        }

        $connectionName = config("database.default");
        $dbConfig = config("database.connections.{$connectionName}");

        $dbHost = Arr::get(
            $dbConfig,
            'read.host.0',
            Arr::get(
                $dbConfig,
                'read.host',
                Arr::get($dbConfig, 'host')
            )
        );

        $this->dropAllCurrentTables();
        $quote = stripos(PHP_OS, 'WIN') === 0 ? '"' : "'";

        switch ($dbConfig['driver']) {
            case 'pgsql':
                $localSnapshotPath = $this->downloadSnapshot();

                $credentialsFileHandle = tmpfile();
                fwrite($credentialsFileHandle, implode(':', [
                    $dbHost,
                    $dbConfig['port'],
                    $dbConfig['username'],
                    $dbConfig['username'],
                    $dbConfig['password'],
                ]));

                $args = [
                    "{$quote}pg_restore{$quote}",
                    "-U {$dbConfig['username']}",
                    "-h {$dbHost}",
                    "-p {$dbConfig['port']}",
                    "--dbname=$quote{$dbConfig['database']}$quote",
                    config("database.connections.{$connectionName}.restore.add_extra_option", ''),
                    "$quote{$localSnapshotPath}$quote"
                ];

                $process = Process::fromShellCommandline(implode(' ', $args), null, [
                    'PGPASSFILE' => stream_get_meta_data($credentialsFileHandle)['uri'],
                    'PGDATABASE' => $dbConfig['database'],
                ], null);

                $process->run(static function ($type, $buffer) {
                    echo ($buffer);
                });

                $this->deleteDownloadedSnapshot($localSnapshotPath);
                break;
            default:
                $dbDumpContents = $this->disk->get($this->fileName);

                if ($this->compressionExtension === 'gz') {
                    $dbDumpContents = gzdecode($dbDumpContents);
                }

                DB::connection($connectionName)->unprepared($dbDumpContents);
                break;
        }

        event(new LoadedSnapshot($this));
    }

    public function delete()
    {
        event(new DeletingSnapshot($this));

        $this->disk->delete($this->fileName);

        event(new DeletedSnapshot($this->fileName, $this->disk));
    }

    public function size(): int
    {
        return $this->disk->size($this->fileName);
    }

    public function createdAt(): Carbon
    {
        return Carbon::createFromTimestamp($this->disk->lastModified($this->fileName));
    }

    protected function dropAllCurrentTables()
    {
        DB::connection(DB::getDefaultConnection())
            ->getSchemaBuilder()
            ->dropAllTables();

        DB::reconnect();
    }

    protected function downloadSnapshot() {
        $directory = (new TemporaryDirectory(config('db-snapshots.temporary_directory_path')))->create();
        $downloadPath = $directory->path($this->fileName);

        event(new DownloadingSnapshot($this, $downloadPath));
        $inputHandle = $this->disk->readStream($this->fileName);
        $outputHandle = fopen($downloadPath, 'w');

        while (!feof($inputHandle)) {
            $buffer = fread($inputHandle, self::CHUNK_SIZE);
            fwrite($outputHandle, $buffer);
        }
        fclose($inputHandle);

        fflush($outputHandle);
        fclose($outputHandle);
        event(new DownloadedSnapshot($this, $downloadPath));

        return $downloadPath;
    }

    protected function deleteDownloadedSnapshot($localSnapshotPath) {
        unlink($localSnapshotPath);
        rmdir(dirname($localSnapshotPath));
    }
}
